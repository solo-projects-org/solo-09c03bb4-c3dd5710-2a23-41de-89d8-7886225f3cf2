"""MQTT 客户端：QoS1 订阅模拟秤报文。

- 持久会话（clean_session=False），断线期间 broker 缓存未达的 QoS1 消息，
  重连后重投；报文入库按 (startup_id, seq) 幂等，重投不会产生重复采样；
- 每条消息在一个 DB 事务里入库并推进状态机；
- retain=True 的报文直接丢弃，绝不作为实时采样。
"""
from __future__ import annotations

import logging

import paho.mqtt.client as mqtt
from pydantic import ValidationError

from .config import settings
from .db import session_scope
from .engine import record_sample

logger = logging.getLogger("weighbridge.mqtt")


class MqttScaleClient:
    def __init__(self) -> None:
        # paho-mqtt 2.x Callback API
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=settings.mqtt_client_id,
            clean_session=False,
            protocol=mqtt.MQTTv311,
        )
        if settings.mqtt_username:
            self._client.username_pw_set(
                settings.mqtt_username, settings.mqtt_password or None
            )
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect
        self._client.on_subscribe = self._on_subscribe
        self._stopping = False
        self.connected = False
        self.subscribed = False

    def start(self) -> None:
        logger.info("连接 MQTT broker %s:%s", settings.mqtt_host, settings.mqtt_port)
        self._client.connect(
            settings.mqtt_host, settings.mqtt_port, settings.mqtt_keepalive
        )
        self._client.loop_start()

    def wait_ready(self, timeout: float = 10.0) -> bool:
        import time

        deadline = time.time() + timeout
        while time.time() < deadline and not self.subscribed:
            time.sleep(0.05)
        return self.subscribed

    def stop(self) -> None:
        if self._stopping:
            return
        self._stopping = True
        self._client.loop_stop()
        try:
            self._client.disconnect()
        except Exception:  # pragma: no cover - 已断开时忽略
            pass
        self.connected = False
        self.subscribed = False

    def _on_connect(self, client, _userdata, _flags, reason_code, _properties):
        if reason_code == 0:
            self.connected = True
            # 每次（重）连都重新订阅，QoS1；SUBACK 到达后置位 subscribed
            self.subscribed = False
            client.subscribe(settings.mqtt_topic, qos=1)
            logger.info("已订阅 %s (QoS1)", settings.mqtt_topic)
        else:
            logger.error("MQTT 连接失败: %s", reason_code)

    def _on_subscribe(self, _client, _userdata, _mid, _reason_codes, _properties):
        self.subscribed = True

    def _on_disconnect(self, _client, _userdata, _flags, reason_code, _properties):
        self.connected = False
        self.subscribed = False
        if not self._stopping:
            logger.warning("MQTT 断开（%s），paho 将自动重连", reason_code)

    def _on_message(self, _client, _userdata, msg):
        retained = bool(msg.retain)
        if retained:
            # 保留消息不算实时采样，直接丢弃（也不回 PUBACK 之前的任何处理）
            logger.info("丢弃 retained 报文: %s", msg.payload)
            return
        try:
            from .schemas import ScaleSampleIn

            payload = ScaleSampleIn.model_validate_json(msg.payload)
        except ValidationError as exc:
            logger.error("非法报文 %s: %s", msg.payload, exc)
            return

        with session_scope() as session:
            sample = record_sample(
                session,
                startup_id=payload.startup_id,
                seq=payload.seq,
                weight_kg=payload.weight_kg,
                stable=payload.stable,
                retained=retained,
            )
        # 测试钩子：每条实时采样处理完成后通知，替代不稳定的固定 sleep
        hook = getattr(self, "on_sample_processed", None)
        if hook is not None and sample is not None:
            hook(payload.startup_id, payload.seq)
        logger.info(
            "采样入库 startup=%s seq=%s weight=%s stable=%s",
            payload.startup_id,
            payload.seq,
            payload.weight_kg,
            payload.stable,
        )


# 允许测试时替换客户端实现
_client_singleton: MqttScaleClient | None = None


def get_mqtt_client() -> MqttScaleClient:
    global _client_singleton
    if _client_singleton is None:
        _client_singleton = MqttScaleClient()
    return _client_singleton
