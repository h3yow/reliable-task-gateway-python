# Reliable Task Gateway

具备可靠投递语义的后台任务网关：在重复提交、并发提交与下游间歇故障下，
保证任务状态与结果可预期。

## 架构

```
main.py            HTTP 入口（FastAPI 路由）
app/models.py      任务模型与状态机定义
app/store.py       SQLite 本地持久化（幂等插入、条件迁移）
app/service.py     网关服务：幂等提交 + 状态机执行 + 重试/熔断编排
app/downstream.py  下游调用抽象、错误分类、重试策略
app/breaker.py     熔断器（CLOSED / OPEN / HALF_OPEN）
tests/             可靠性测试
```

## 幂等语义

- 提交接口 `POST /tasks` 以 `idempotency_key`（业务键）作为幂等键，
  数据库层面对该键建有唯一约束。
- **重复提交**（同键同载荷）：幂等命中，返回与首次完全一致的结果
  （同一 `task_id`、状态与 `result`），响应 `outcome=DUPLICATE`，
  下游不会被再次调用。并发重复提交时仅有一个请求真正执行，
  其余请求等待首次执行结束后返回相同结果。
- **冲突提交**（同键不同载荷）：明确拒绝，HTTP 409，
  `detail.reason=PAYLOAD_CONFLICT`，与重复提交（201 + `DUPLICATE`）可明确区分。

## 状态机

```
PENDING ──▶ RUNNING ──▶ SUCCEEDED   (终态)
   │          │  ├────▶ FAILED      (终态：不可重试失败 / 重试耗尽)
   │          │  ├────▶ CIRCUIT_REJECTED (终态：熔断拒绝)
   │          │  ├────▶ TIMED_OUT   (终态：超过最长执行时限)
   │          │  └────▶ CANCELLED   (终态：调用方主动取消)
   │          └────▶ RUNNING (自环，仅用于重试时递增 attempts)
   ├─────────▶ CIRCUIT_REJECTED
   ├─────────▶ TIMED_OUT
   └─────────▶ CANCELLED
```

- 迁移合法性由 `app/models.py` 的 `ALLOWED_TRANSITIONS` 校验，非法迁移抛
  `IllegalTransitionError`；**终态不可变更**。
- 状态迁移通过条件 UPDATE（`WHERE status = expected`）实现乐观并发控制，
  并发更新不会丢更新；条件不满足即抛错。
- 所有状态持久化于本地 SQLite（WAL 模式），进程重启后已提交任务的状态
  与结果不丢失、不回退。

## 重试与熔断规则

**错误分类**（`app/downstream.py`）：

| 错误类型 | 含义 | 处理 |
|---|---|---|
| `RetryableError` | 超时、5xx、连接错误 | 指数退避重试，计入熔断器 |
| `NonRetryableError` | 4xx 业务校验失败 | 不重试，直接终态 `FAILED`，不计入熔断器 |

**重试**：默认最多 3 次尝试，指数退避（`base_delay * 2^(n-1)`）。
重试耗尽后任务进入 `FAILED`，`error` 前缀为 `RETRY_EXHAUSTED:`。

**熔断**（`app/breaker.py`）：

- `CLOSED`：正常调用；连续失败达到阈值（默认 3 次）转为 `OPEN`。
- `OPEN`：直接拒绝调用，任务进入终态 `CIRCUIT_REJECTED`，
  `error` 前缀为 `CIRCUIT_OPEN:`，不调用下游、不放大流量。
- 超过恢复超时（默认 30s）后转为 `HALF_OPEN`，**仅放行一个探测请求**，
  探测期间其余请求仍被拒绝（防止流量放大与重复副作用）；
  探测成功回到 `CLOSED`，失败回到 `OPEN` 并重新计时。

**失败原因可区分**：`NON_RETRYABLE:` / `RETRY_EXHAUSTED:` / `CIRCUIT_OPEN:`
三种前缀分别对应不可重试失败、重试耗尽、熔断拒绝；超时与取消另有
`TIMEOUT:` / `CANCELLED:` 前缀（见下节），五种收尾原因互不混淆。

## 超时（最长执行时限）

- 提交时可选传 `timeout_seconds`（秒，必须为正）。网关在提交时刻计算
  **绝对截止时刻** `deadline_at = 提交时刻 + 时限` 并持久化，
  因此**进程重启后仍按原截止时间继续计算**，不存在重启后续期或重算。
- 到点未跑完的任务自动收尾为终态 `TIMED_OUT`，`error` 前缀为 `TIMEOUT:`，
  与 `FAILED`（含重试耗尽）、`CIRCUIT_REJECTED` 明确区分。
- 判定时机：每次下游调用前、每次重试退避后、每次下游调用返回后。
  重试退避会被截断到剩余时限，到点即收尾，不再消耗剩余重试次数。
- **不产生重复副作用**：`TIMED_OUT` 是终态，状态机不允许其迁出；
  超时收尾后重复提交只幂等命中（`DUPLICATE`），下游不会被重新触发。
  在途的下游调用若在时限后返回，按超时收尾、不记入迟到结果，
  也不会再触发新的尝试。
- **兼容**：不传 `timeout_seconds` 的老提交方式 `deadline_at` 为 NULL，
  永不超时，行为与旧版完全一致；重复提交以首次提交的时限为准。

## 取消

- `POST /tasks/{idempotency_key}/cancel` 主动取消未收尾任务。
- **幂等**：可重复调用。已处于 `CANCELLED` 的任务再次取消返回相同状态，
  `cancel_outcome=CANCELLED`，不报错、结果一致。
- **不覆盖既有结果**：任务已收尾为其他终态（`SUCCEEDED`/`FAILED`/
  `CIRCUIT_REJECTED`/`TIMED_OUT`）时，取消返回 `cancel_outcome=ALREADY_TERMINAL`
  及任务的**真实状态与结果**，不覆盖、不假装取消成功。
- **撞车唯一终态**：取消与正常完成并发时，双方走同一条条件 UPDATE
  （`WHERE status = expected`），先到者赢——要么 `CANCELLED` 要么
  `SUCCEEDED`/`FAILED`，不会出现结果丢失、状态回退或双终态；
  落败方重读存储返回实际终态。
- **持久化意图**：取消先把 `cancel_requested` 标记落库再做状态迁移，
  进程在两者之间崩溃时，重启后由 `sweep()` 继续完成取消。
- 对在途任务取消后，执行循环在下一次尝试前发现取消标记即收尾，
  不再触发新的下游调用；已发出的在途调用返回后也无法把
  `CANCELLED` 覆盖为其他状态。

## 重启恢复（超时与取消）

- 启动时（`main.py` startup）调用 `gateway.sweep()`：对所有未收尾任务，
  落实已持久化的取消意图（→ `CANCELLED`）、按原 `deadline_at`
  收尾已超时任务（→ `TIMED_OUT`）。
- 已收尾任务（含 `TIMED_OUT`/`CANCELLED`）重启后不可变更，
  清扫不会触碰终态任务。

## 升级兼容

- 库表新增 `deadline_at`、`cancel_requested` 两列，启动时自动
  `ALTER TABLE` 补列（幂等），升级前已存下的任务无需迁移脚本：
  老任务 `deadline_at=NULL`（不限时）、`cancel_requested=0`，
  行为与升级前一致，且可直接使用取消能力。

## API

- `POST /tasks` `{idempotency_key, payload, timeout_seconds?}` → 201，
  任务视图 + `outcome`（`CREATED`/`DUPLICATE`）；冲突载荷返回 409
  `PAYLOAD_CONFLICT`。`timeout_seconds` 可选（正数，秒），不传则不限时。
- `GET /tasks/{idempotency_key}` → 任务当前状态与结果。
- `POST /tasks/{idempotency_key}/cancel` → 任务视图 + `cancel_outcome`
  （`CANCELLED`/`ALREADY_TERMINAL`）；任务不存在返回 404。

任务视图包含 `deadline_at`（绝对截止时刻，NULL 表示不限时）与
`cancel_requested`（已持久化的取消意图）。

运行：

```bash
uv sync
uv run uvicorn main:app
```

## 验证方法

```bash
uv run pytest tests/ -v -o log_cli=true
```

测试覆盖（`tests/test_gateway.py`）：

1. **并发重复提交**：16 线程同时提交同键同载荷，断言只产生一个任务、
   下游仅调用一次、所有提交方拿到一致结果。
2. **冲突载荷**：同键不同载荷抛 `PayloadConflictError`，与重复提交可区分。
3. **重启恢复**：关闭并重开数据库模拟进程重启，断言状态/结果不丢失不回退，
   重启后重复提交不再调用下游；终态在重启后仍不可变更。
4. **熔断状态转换**：连续失败打开熔断器，后续任务以 `CIRCUIT_OPEN` 拒绝；
   恢复超时后单探测成功关闭熔断器，探测期间不放大流量。

生命周期测试（`tests/test_lifecycle.py`）：

5. **超时终止**：下游调用在时限后返回 → `TIMED_OUT`（`TIMEOUT:` 前缀），
   与 `FAILED`/`CIRCUIT_REJECTED` 区分；重试退避期间到点即停止重试；
   超时收尾后重复提交幂等命中、下游不被重新触发；不带时限的提交行为不变。
6. **取消幂等**：在途任务取消后重复取消结果一致；在途调用返回不覆盖
   `CANCELLED`、不落下迟到结果、下游仅调用一次；已成功/已失败任务取消
   返回 `ALREADY_TERMINAL` 与真实状态，既有结果保留。
7. **取消与完成撞车**：30 次并发撞车实验，每次终态均为
   `CANCELLED`/`SUCCEEDED` 之一且字段自洽，撞车后状态不回退。
8. **重启后语义保持**：重启前定下的时限按原截止时间继续计算；
   重启前落库的取消意图由 `sweep()` 继续落实；`TIMED_OUT`/`CANCELLED`
   终态重启后不可改回。
9. **升级兼容**：手工构造旧版库表（无新列），打开即自动补列，
   老任务结果保留、无时限、不被超时清扫、可正常取消。

测试日志（`-o log_cli=true`）会打印业务键（`key=...`）、每次状态迁移
（`状态迁移 X -> Y`）与每个用例的判定依据（`判定依据: ...`）。
