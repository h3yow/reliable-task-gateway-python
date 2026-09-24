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
   │          │  └────▶ CIRCUIT_REJECTED (终态：熔断拒绝)
   │          └────▶ RUNNING (自环，仅用于重试时递增 attempts)
   └─────────▶ CIRCUIT_REJECTED
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
三种前缀分别对应不可重试失败、重试耗尽、熔断拒绝。

## API

- `POST /tasks` `{idempotency_key, payload}` → 201，任务视图 + `outcome`
  （`CREATED`/`DUPLICATE`）；冲突载荷返回 409 `PAYLOAD_CONFLICT`。
- `GET /tasks/{idempotency_key}` → 任务当前状态与结果。

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

测试日志（`-o log_cli=true`）会打印业务键（`key=...`）、每次状态迁移
（`状态迁移 X -> Y`）与每个用例的判定依据（`判定依据: ...`）。
