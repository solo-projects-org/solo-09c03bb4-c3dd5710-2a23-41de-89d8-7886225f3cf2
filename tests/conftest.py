"""pytest 公共夹具：内存 SQLite、amqtt 内存 broker、MQTT 发布者。"""
from __future__ import annotations

import asyncio
import json
import socket
import threading

import pytest

from app import db
from app.config import settings


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture()
def mem_db(monkeypatch):
    """每个测试一个全新的内存库。"""
    monkeypatch.setattr(settings, "db_path", "sqlite:///:memory:")
    db.reinit_engine()
    db.init_db()
    yield
    db.get_engine().dispose()


class _BrokerThread(threading.Thread):
    def __init__(self, port: int):
        super().__init__(daemon=True)
        self.port = port
        self.ready = threading.Event()
        self._stop_flag = threading.Event()
        self.error: Exception | None = None

    def request_stop(self):
        self._stop_flag.set()

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def _serve():
            from amqtt.broker import Broker

            config = {
                "listeners": {
                    "default": {
                        "type": "tcp",
                        "bind": f"127.0.0.1:{self.port}",
                        "max_connections": 50,
                    }
                },
                # 测试环境调度可能不及时，放宽 broker 端 QoS1 PUBACK 等待
                "qos1_puback_timeout": 30,
                "plugins": {
                    "amqtt.plugins.authentication.AnonymousAuthPlugin": {
                        "allow_anonymous": True
                    },
                },
            }
            broker = Broker(config)
            await broker.start()
            self.ready.set()
            try:
                while not self._stop_flag.is_set():
                    await asyncio.sleep(0.1)
            finally:
                await broker.shutdown()
        try:
            loop.run_until_complete(_serve())
        except Exception as exc:  # pragma: no cover - 诊断用
            self.error = exc
            self.ready.set()

    def stop(self):
        self._stop = True


@pytest.fixture(scope="session")
def mqtt_broker():
    port = _free_port()
    thread = _BrokerThread(port)
    thread.start()
    thread.ready.wait(timeout=10)
    if thread.error:
        pytest.skip(f"amqtt broker 启动失败: {thread.error}")
    yield port
    thread.request_stop()
    thread.join(timeout=5)


@pytest.fixture()
def mqtt_port(monkeypatch, mqtt_broker):
    monkeypatch.setattr(settings, "mqtt_host", "127.0.0.1")
    monkeypatch.setattr(settings, "mqtt_port", mqtt_broker)
    return mqtt_broker


class MqttPublisher:
    """测试用 QoS1 发布者。"""

    def __init__(self, port: int, topic: str):
        import paho.mqtt.client as mqtt

        self.topic = topic
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"test-pub-{id(self)}",
            clean_session=True,
        )
        self._connected = threading.Event()
        self.client.on_connect = lambda *_: self._connected.set()
        self.client.connect("127.0.0.1", port)
        self.client.loop_start()
        if not self._connected.wait(timeout=10):
            raise RuntimeError("测试发布者连接 broker 超时")

    def send(self, startup_id: int, seq: int, weight: float, stable: bool,
             *, retain: bool = False):
        info = self.client.publish(
            self.topic,
            json.dumps(
                {
                    "startup_id": startup_id,
                    "seq": seq,
                    "weight_kg": weight,
                    "stable": stable,
                }
            ),
            qos=1,
            retain=retain,
        )
        info.wait_for_publish(timeout=10)

    def close(self):
        self.client.loop_stop()
        self.client.disconnect()


@pytest.fixture()
def publisher(mqtt_port):
    pub = MqttPublisher(mqtt_port, settings.mqtt_topic)
    yield pub
    pub.close()


def wait_until(predicate, timeout: float = 10.0, interval: float = 0.05):
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False
