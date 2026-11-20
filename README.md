# 单磅地磅毛重采集服务

送粮车卸货前，磅房用本服务把地磅采到的**毛重**记到司机扫出的运输单上。

- **FastAPI** 提供扫码、查单、查采样接口（Swagger：`/docs`）
- **SQLite** 持久化运输单（`tickets`）与采样（`samples`），重启可接续
- **Mosquitto + MQTT QoS1** 接收模拟秤上报：启动号、采样号、公斤数、稳定标记
- **Docker Compose** 一条命令起 API + Mosquitto；模拟秤按需运行

## 目录结构

```
app/                  FastAPI 服务
  config.py           环境变量配置
  db.py               SQLite 引擎/会话（WAL + 库级写锁）
  models.py           Sample / Ticket ORM
  schemas.py          Pydantic 报文与响应模型
  engine.py           称重业务状态机（核心规则）
  mqtt_client.py      QoS1 订阅客户端（持久会话、retain 丢弃）
  routers.py          HTTP 路由
  main.py             应用入口（建库、重启接续、MQTT 生命周期）
simulator/            模拟秤（交互 REPL + 场景脚本）
scenarios/            预定义 JSON 场景
mosquitto/            broker 配置
tests/                单元测试 + HTTP 测试 + 真实 MQTT 端到端测试
```

## 快速开始（Docker Compose）

```bash
docker compose up -d --build
# API:   http://localhost:8000/docs
# MQTT:  localhost:1883
```

跑一个场景（稳定读数先到、回零读数后补）：

```bash
docker compose run --rm simulator \
  --host mosquitto --scenario scenarios/scenario_stable_first.json
# 然后在 Swagger 调 POST /api/tickets/scan/{transport_code}
```

交互模拟秤：

```bash
docker compose run --rm simulator --host mosquitto
# startup / zero / w 3200 stable / seq 2 / retain 0 / help
```

## 本地开发（无 Docker）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
MQTT_HOST=localhost WEIGH_DB_PATH=./data/demo.db \
  uvicorn app.main:app --reload
python -m simulator.simulator --host localhost
```

## 业务规则与状态机

一张单的生命周期：`active（称重中）→ completed（毛重已定，终态）`，
异常时进入 `reweigh（待重称）`，空磅重扫后回到 `active`。

1. **一台秤一次只称一张单**：存在 active/reweigh 单时，其它运输单扫码返回
   `409 ScaleBusy`，从入口杜绝串单。
2. **空磅扫码开单**：必须锚定“最新的实时零读数”。
   - MQTT `retain=True` 的保留消息不算实时采样，直接丢弃、不入库；
   - 零读数全局唯一使用（DB 唯一约束 + 占用查询双保险）；
   - 没有可用实时零读数 → `409 NoRealTimeZero`。
3. **取毛重**：从锚定零读数起，在**同一次启动**的**连续采样**中取首个
   **稳定正重**（`stable=true 且 weight>0`）。
   - 采样号缺号时**先等补报**：游标以 `(startup_id, seq)` 推进，补报入库后
     自动补齐，不会跳过缺号也不会误采后面的读数；
   - 不稳定的正重继续等稳定帧；空磅阶段的连续零读数忽略。
4. **待重称（reweigh）**：
   - 上磅后、记重前出现回零（零读数之前已出现过非零采样）；
   - 秤在记重前重启（出现更大的 `startup_id`）。
   原单保留，空磅出现新实时零读数后，原运输单重扫即可重新启用。
5. **终态保护**：毛重写入成功才置 `completed`；之后任何采样（含迟到的回零）
   都不改写，重扫只返回原毛重。
6. **重启接续**：单据游标 `last_sample_id` 落库；启动时对所有 active 单执行
   `resume_after_restart`，用已落库采样继续推进；MQTT 持久会话
   （`clean_session=false`）+ QoS1 让断线期间报文由 broker 缓存重投。

### 并发与一致性

- 所有状态推进在进程内 `db_lock` 下串行，HTTP 线程与 MQTT 回调线程不会交错改单；
- 采样入库 `(startup_id, seq)` 唯一，重复报文幂等；
- SQLite 开 WAL、`busy_timeout=30s`，数据在 `/data` 卷持久化。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/tickets/scan/{transport_code}` | 扫码：开单/重扫/查询已完成单 |
| GET  | `/api/tickets` | 查全部单 |
| GET  | `/api/tickets/{id}` | 查单 |
| GET  | `/api/samples?limit=200` | 查采样（最新在前） |
| GET  | `/health` | 健康检查 |

扫码响应 `result`：`opened`（新开单）/ `re_armed`（待重称单重新启用）/
`already_active`（本单称重中重扫）/ `completed`（终态重扫，只回原毛重）；
磅被占用或没有实时零读数时返回 `409`。

## 模拟秤报文

Topic：`scale/weighbridge`，QoS 1，JSON：

```json
{"startup_id": 1, "seq": 0, "weight_kg": 3200.0, "stable": true}
```

启动号每次开机递增；每次启动内 `seq` 从 0 连续编号。

## 测试

```bash
pytest -q          # 23 项：状态机规则 13 + HTTP 5 + 真实 MQTT 端到端 4 + 其余 1
```

端到端用内存 MQTT broker（amqtt）跑真实 QoS1 链路，覆盖：稳定先到/回零后补、
缺号补报、retain 丢弃、回零重扫。
