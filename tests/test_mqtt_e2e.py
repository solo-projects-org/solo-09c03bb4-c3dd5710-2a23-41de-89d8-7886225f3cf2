"""端到端：真实 MQTT broker（amqtt）+ QoS1 发布 + FastAPI 入库成单。

重点试跑题目指定情形：稳定读数先到、回零读数后补。
测试通过“采样处理事件”做确定性同步，避免固定 sleep 造成的时序抖动。
"""
from __future__ import annotations

import importlib
import threading
import time
import uuid

import pytest

import app.mqtt_client as mqtt_mod


class SampleSync:
    """记录应用端已处理完的采样 (startup_id, seq)，供测试精确等待。"""

    def __init__(self):
        self._seen: set[tuple[int, int]] = set()
        self._cond = threading.Condition()

    def hook(self, startup_id: int, seq: int):
        with self._cond:
            self._seen.add((startup_id, seq))
            self._cond.notify_all()

    def wait_processed(self, pairs, timeout: float = 10.0) -> bool:
        wanted = set(pairs)
        deadline = time.time() + timeout
        with self._cond:
            while not wanted.issubset(self._seen):
                remaining = deadline - time.time()
                if remaining <= 0:
                    missing = wanted - self._seen
                    raise AssertionError(
                        f"超时未收到采样处理事件，缺失={sorted(missing)}，"
                        f"已处理={sorted(self._seen)}，"
                        f"连接状态 connected={mqtt_mod.get_mqtt_client().connected} "
                        f"subscribed={mqtt_mod.get_mqtt_client().subscribed}"
                    )
                self._cond.wait(remaining)
            return True

    @property
    def seen(self):
        with self._cond:
            return set(self._seen)


@pytest.fixture()
def live_app(mem_db, mqtt_port, monkeypatch):
    from app.config import settings

    monkeypatch.delenv("DISABLE_MQTT", raising=False)
    # 每个测试用独立 client id，避免 broker 里持久会话互相串
    monkeypatch.setattr(
        settings, "mqtt_client_id", f"test-api-{uuid.uuid4().hex}"
    )
    import app.main as main_mod

    old = mqtt_mod._client_singleton
    if old is not None:
        old.stop()
    mqtt_mod._client_singleton = None
    importlib.reload(main_mod)

    from fastapi.testclient import TestClient

    sync = SampleSync()
    with TestClient(main_mod.app) as client:
        mqtt_client = mqtt_mod.get_mqtt_client()
        mqtt_client.wait_ready()
        mqtt_client.on_sample_processed = sync.hook
        yield client, sync
    mqtt_mod._client_singleton = None


def _sample_count(client) -> int:
    return len(client.get("/api/samples").json())


def test_mqtt_qos1_end_to_end_stable_first_zero_later(live_app, publisher):
    """题目指定：稳定读数先到、回零读数后补。"""
    client, sync = live_app

    # 1) 实时零读数到达并被处理，再扫码开单
    publisher.send(1, 0, 0.0, True)
    assert sync.wait_processed([(1, 0)])
    r = client.post("/api/tickets/scan/E2E-1")
    assert r.json()["result"] == "opened"

    # 2) 不稳定帧后，稳定正重先到
    publisher.send(1, 1, 3100.0, False)
    publisher.send(1, 2, 3200.0, True)
    assert sync.wait_processed([(1, 1), (1, 2)])

    # 处理事件返回时状态机已在同一事务里推进完
    ticket = client.get("/api/tickets").json()[0]
    assert ticket["status"] == "completed"
    assert ticket["gross_weight_kg"] == 3200.0

    # 3) 回零读数后补：单已完成，不允许改写毛重
    publisher.send(1, 3, 0.0, True)
    assert sync.wait_processed([(1, 3)])
    ticket = client.get("/api/tickets").json()[0]
    assert ticket["status"] == "completed"
    assert ticket["gross_weight_kg"] == 3200.0

    # 重扫只回原毛重
    again = client.post("/api/tickets/scan/E2E-1").json()
    assert again["result"] == "completed"
    assert again["ticket"]["gross_weight_kg"] == 3200.0


def test_mqtt_gap_backfill_over_the_wire(live_app, publisher):
    """缺号先等补报：报文经真实 MQTT 链路乱序到达。"""
    client, sync = live_app
    publisher.send(1, 0, 0.0, True)
    assert sync.wait_processed([(1, 0)])
    client.post("/api/tickets/scan/E2E-2")

    publisher.send(1, 1, 3100.0, False)
    publisher.send(1, 3, 3250.0, True)   # 缺 seq=2
    assert sync.wait_processed([(1, 1), (1, 3)])
    ticket = client.get("/api/tickets").json()[0]
    assert ticket["status"] == "active"  # 缺号，先等补报

    publisher.send(1, 2, 3180.0, False)  # 补报
    assert sync.wait_processed([(1, 2)])
    ticket = client.get("/api/tickets").json()[0]
    assert ticket["status"] == "completed"
    assert ticket["gross_weight_kg"] == 3250.0


def test_mqtt_retained_messages_are_dropped(
    live_app, mqtt_port, monkeypatch
):
    """retain 报文不算实时采样：新订阅时 broker 投递的保留消息必须丢弃。

    MQTT 语义：RETAIN 标志只在“新订阅建立、broker 投递存储消息”时置位；
    因此先在独立主题上存好保留消息，再让应用端订阅。
    """
    from app.config import settings

    client, _sync = live_app
    unique_topic = f"test/retained/{uuid.uuid4().hex}"
    monkeypatch.setattr(settings, "mqtt_topic", unique_topic)

    from tests.conftest import MqttPublisher

    # 1) 应用端尚未订阅该主题：先存两条保留消息
    isolated_pub = MqttPublisher(mqtt_port, unique_topic)
    try:
        isolated_pub.send(1, 0, 0.0, True, retain=True)
        isolated_pub.send(1, 1, 3200.0, True, retain=True)
        time.sleep(0.3)

        # 2) 重启应用内 MQTT 客户端，订阅即收到保留消息（retain=True 被丢弃）
        old = mqtt_mod._client_singleton
        if old is not None:
            old.stop()
        mqtt_mod._client_singleton = None
        new_client = mqtt_mod.get_mqtt_client()
        new_client.start()
        assert new_client.wait_ready(), "应用端 MQTT 订阅未就绪"

        # 3) 保留消息一律不入库：轮询整个投递窗口确认采样表始终为空
        deadline = time.time() + 1.5
        while time.time() < deadline:
            assert _sample_count(client) == 0
            time.sleep(0.05)
        r = client.post("/api/tickets/scan/E2E-3")
        assert r.status_code == 409
    finally:
        isolated_pub.close()
        mqtt_mod.get_mqtt_client().stop()
        mqtt_mod._client_singleton = None


def test_mqtt_return_zero_then_rescan(live_app, publisher):
    """上磅后回零 → 待重称 → 空磅重扫 → 新稳定正重完成。"""
    client, sync = live_app
    publisher.send(1, 0, 0.0, True)
    assert sync.wait_processed([(1, 0)])
    client.post("/api/tickets/scan/E2E-4")

    publisher.send(1, 1, 2900.0, False)
    publisher.send(1, 2, 0.0, True)
    assert sync.wait_processed([(1, 1), (1, 2)])
    assert client.get("/api/tickets").json()[0]["status"] == "reweigh"

    publisher.send(1, 3, 0.0, True)
    assert sync.wait_processed([(1, 3)])
    r = client.post("/api/tickets/scan/E2E-4")
    assert r.json()["result"] == "re_armed"

    publisher.send(1, 4, 3300.0, True)
    assert sync.wait_processed([(1, 4)])
    ticket = client.get("/api/tickets").json()[0]
    assert ticket["status"] == "completed"
    assert ticket["gross_weight_kg"] == 3300.0
