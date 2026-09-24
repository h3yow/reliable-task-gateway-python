# reliable-task-gateway

具备可靠投递语义的后台任务网关：在重复提交、并发提交与下游间歇故障下，
任务状态与结果保持可预期。

## 架构

```
app/
  models.py      任务模型与状态机（合法迁移表 + 校验）
  store.py       SQLite 本地持久化：幂等插入、乐观锁迁移、重启恢复
  breaker.py     熔断器：CLOSED / OPEN / HALF_OPEN
  downstream.py  下游调用抽象与可重试/不可重试错误分类
  service.py     编排：幂等提交 + 状态机 + 重试 + 熔断
  main.py        FastAPI 接口层
```

## 幂等语义

- 提交任务必须携带业务幂等键 `idempotency_key`；载荷以规范化 JSON 的
  SHA-256 作为 `payload_hash` 一并持久化。
- **重复提交**（同键同载荷，无论串行还是并发）：返回与首次完全一致的结果
  （同一 `task_id`、状态与 `result`），下游只执行一次。并发场景下后到者
  会等待首个执行到达终态后再返回，保证结果一致。
- **冲突载荷**（同键不同载荷）：明确拒绝——服务层抛 `PayloadConflictError`，
  HTTP 返回 `409`；与重复提交的 `200` 路径明确区分，且不触发新的下游调用、
  不污染原任务。

## 状态机

```
PENDING --> RUNNING --> SUCCEEDED
   |          |
   +----------+--------> FAILED
```

- 合法迁移仅：`PENDING→RUNNING`、`PENDING→FAILED`、`RUNNING→SUCCEEDED`、
  `RUNNING→FAILED`；`SUCCEEDED` / `FAILED` 为终态，**不可再变更**。
- 非法迁移抛 `IllegalTransitionError`。
- 持久化使用 SQLite（WAL + `synchronous=FULL`）。所有迁移在
  `BEGIN IMMEDIATE` 事务内完成，并携带版本号做乐观锁校验
  （`UPDATE ... WHERE version=?`），并发更新不丢更新；版本失配抛
  `ConcurrentUpdateError`。
- **重启恢复**：进程重启时 `recover()` 将残留 `RUNNING` 任务回退为
  `PENDING` 并重新驱动执行；终态任务的状态与结果不丢失、不回退、不重跑。
  注意：对下游是 at-least-once——若崩溃发生在下游调用成功但结果落库之前，
  恢复后可能重放一次调用，下游需自行幂等。

## 重试与熔断

- 下游错误分类：
  - `RetryableError`（超时、5xx、连接错误）：按 `max_retries` 重试，
    线性退避；计入熔断失败计数。
  - `NonRetryableError`（4xx 业务校验）：立即置任务 `FAILED`，**不重试、
    不计入熔断**。
- 熔断器：连续失败达 `failure_threshold` 次后 `OPEN`；`OPEN` 期间直接拒绝，
  不调用下游；冷却 `recovery_timeout` 秒后进入 `HALF_OPEN`，**只放行一个
  探测请求**（不放大流量、不产生重复副作用）；探测成功则 `CLOSED`，失败则
  重新 `OPEN` 并重新计时。
- 失败原因（`Task.error` 前缀）互相可区分：
  | 前缀 | 含义 |
  |---|---|
  | `non_retryable:` | 下游返回不可重试错误 |
  | `retry_exhausted:` | 可重试错误在重试预算内未恢复 |
  | `circuit_open:` | 熔断器 OPEN，本次未调用下游 |

## HTTP 接口

- `POST /tasks` `{idempotency_key, payload}` → `200` 任务结果（重复提交同样
  `200` 且响应体一致）；冲突载荷 → `409`。
- `GET /tasks/{idempotency_key}` → 任务当前状态与结果；不存在 → `404`。
- `GET /health` → 服务与熔断器状态。

运行：

```bash
uv run uvicorn main:app
# 环境变量：GATEWAY_DB（默认 gateway.db）、DOWNSTREAM_URL、
#           BREAKER_THRESHOLD（默认 3）、BREAKER_RECOVERY（默认 30 秒）
```

## 验证方法

```bash
uv run pytest tests/ -v
```

覆盖范围：

- `tests/test_idempotency.py`：16 线程并发重复提交仅一次执行且结果一致；
  冲突载荷拒绝且原因可区分；失败任务的重复提交同样幂等。
- `tests/test_state_machine.py`：合法/非法迁移与终态不可变。
- `tests/test_recovery.py`：模拟崩溃重启后 RUNNING 任务恢复重跑、终态结果
  与失败原因不丢失不回退。
- `tests/test_retry_breaker.py`：可重试/不可重试分类、重试耗尽、熔断
  OPEN→HALF_OPEN→CLOSED/OPEN 转换、探测不放大流量。
- `tests/test_http.py`：HTTP 层 200/409/404 语义与失败原因可区分；
  并断言日志中包含业务键、状态迁移（`PENDING->RUNNING` 等）与判定依据
  （`reason=`）。
