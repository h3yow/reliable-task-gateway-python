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
   └─────────▶ CIRCUIT_REJECTED / TIMED_OUT / CANCELLED
```

- 迁移合法性由 `app/models.py` 的 `ALLOWED_TRANSITIONS` 校验，非法迁移抛
  `IllegalTransitionError`；**终态不可变更**。
- 状态迁移通过条件 UPDATE（`WHERE status = expected`）实现乐观并发控制，
  并发更新不会丢更新；条件不满足即抛错。**完成、取消、超时三方撞车时，
  条件 UPDATE 保证只有一个胜者**，落败方回读并返回真实当前状态，
  最终状态二选一、唯一确定，不会出现结果丢失、状态回退或两个终态并存。
- 所有状态持久化于本地 SQLite（WAL 模式），进程重启后已提交任务的状态
  与结果不丢失、不回退。

## 超时（最长执行时限）

- 提交时可选传 `timeout_seconds`（秒），网关在提交时刻计算**绝对截止时间**
  `deadline_at` 并随任务持久化；进程重启后按原时间继续计算，不重新计时。
- 超时判定有两条路径，互为补充：
  1. **执行循环内**：每次派发下游调用前、以及重试退避结束后检查时限，
     到点即收尾为 `TIMED_OUT`，**不再派发新的下游尝试**（未派发的尝试
     也不计入 `attempts`）；
  2. **后台清扫器**（`start_timeout_sweeper`，默认 0.5s 间隔）：扫描
     `deadline_at` 已过且仍处于 `PENDING/RUNNING` 的任务，收尾为 `TIMED_OUT`。
- 超时任务 `error` 前缀为 `TIMEOUT:`，与 `NON_RETRYABLE:` / `RETRY_EXHAUSTED:` /
  `CIRCUIT_OPEN:` / `CANCELLED:` 明确可区分。
- **不制造重复副作用**：`TIMED_OUT` 是终态，条件迁移保证已发出的在途下游
  调用返回后无法覆盖终态（迟到的结果被丢弃），也绝不会因超时收尾而再次
  触发下游调用。
- **兼容**：不传 `timeout_seconds` 的老提交方式 `deadline_at` 为 NULL，
  不参与超时判定，行为与引入本特性前完全一致。

## 取消

- `POST /tasks/{idempotency_key}/cancel` 主动取消未完成任务，
  状态迁移到终态 `CANCELLED`，`error` 前缀为 `CANCELLED:`。
- **幂等**：重复取消不报错、结果一致——均返回 `cancel_outcome=CANCELLED`
  与同一终态，不产生新的状态变更（`version` 不变）。
- **已到其他终态**（成功/失败/熔断拒绝/超时）：取消**不覆盖已有结果**，
  返回 `cancel_outcome=ALREADY_FINISHED` 与任务的真实当前状态，
  调用方能明确看到任务现在到底是什么状态。
- **与完成撞车**：取消与正常完成/超时并发时，由条件 UPDATE 仲裁唯一胜者；
  取消落败时如实返回 `ALREADY_FINISHED` 与真实终态，不假装取消成功。
- 对在途下游调用的影响：取消立即生效为终态；在途调用返回后其结果被丢弃，
  不会覆盖 `CANCELLED`，也不会触发重试或再次调用下游。
- 取消结果持久化，进程重启后继续生效，`CANCELLED` 不可被改回。

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

**失败原因可区分**：`NON_RETRYABLE:` / `RETRY_EXHAUSTED:` / `CIRCUIT_OPEN:` /
`TIMEOUT:` / `CANCELLED:` 五种前缀分别对应不可重试失败、重试耗尽、熔断拒绝、
超时终止、主动取消。

## API

- `POST /tasks` `{idempotency_key, payload, timeout_seconds?}` → 201，
  任务视图 + `outcome`（`CREATED`/`DUPLICATE`）；冲突载荷返回 409
  `PAYLOAD_CONFLICT`。`timeout_seconds` 可选，不传则行为同老版本。
- `GET /tasks/{idempotency_key}` → 任务当前状态与结果。
- `POST /tasks/{idempotency_key}/cancel` → 200，任务视图 + `cancel_outcome`
  （`CANCELLED`/`ALREADY_FINISHED`）；任务不存在返回 404。

任务视图中的 `deadline_at` 为绝对截止时间（epoch 秒），无时限任务为 `null`。

## 升级兼容

- **老的不带时限的提交**：`timeout_seconds` 缺省时 `deadline_at=NULL`，
  超时清扫器与执行循环的时限检查均跳过，行为与升级前完全一致。
- **升级前已存量的任务**：老库（无 `deadline_at` 列）打开时自动执行
  `ALTER TABLE` 补列，存量任务 `deadline_at` 为 NULL（无时限），
  状态、结果、`attempts` 全部保留，重复提交仍幂等命中。
- 重启恢复：重启前定下的时限按原绝对时间继续计算；重启前已收尾的任务
  （含 `TIMED_OUT` / `CANCELLED`）不可被改回；重启前的取消持续生效。

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

生命周期测试覆盖（`tests/test_lifecycle.py`）：

1. **超时终止**：在途下游调用超时后被清扫器收尾为 `TIMED_OUT`，`TIMEOUT:`
   前缀与其他失败原因可区分；迟到的下游结果不覆盖终态、下游不被重新触发；
   重试退避期间到点不再派发新尝试且不计入 `attempts`；不带时限的老提交
   不受清扫器影响。
2. **幂等取消**：取消 `RUNNING` 任务后重复取消结果一致（`version` 不变）；
   在途调用返回后不覆盖 `CANCELLED`；已成功/已失败任务取消返回
   `ALREADY_FINISHED` 与真实终态，已有结果/失败原因保留。
3. **取消与完成撞车**：50 轮并发取消/提交对撞，每轮终态唯一且自洽
   （`SUCCEEDED` 必带结果、`CANCELLED` 必无结果），撞车后重复取消/重复提交
   均不改变终态，下游调用不超过一次。
4. **重启后语义保持**：重启后按原绝对截止时间继续计算超时（到点前不收尾、
   到点收尾且不触发下游）；重启前的取消持续生效、取消/超时终态不可改回；
   老库（无 `deadline_at` 列）自动迁移，存量任务无时限、行为不变。

测试日志（`-o log_cli=true`）会打印业务键（`key=...`）、每次状态迁移
（`状态迁移 X -> Y`）与每个用例的判定依据（`判定依据: ...`）。
