"""应用配置：全部可通过环境变量覆盖。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass
class Settings:
    db_path: str = _env("WEIGH_DB_PATH", "/data/weighbridge.db")
    mqtt_host: str = _env("MQTT_HOST", "mosquitto")
    mqtt_port: int = int(_env("MQTT_PORT", "1883"))
    mqtt_topic: str = _env("MQTT_TOPIC", "scale/weighbridge")
    mqtt_client_id: str = _env("MQTT_CLIENT_ID", "weighbridge-api")
    mqtt_username: str = _env("MQTT_USERNAME", "")
    mqtt_password: str = _env("MQTT_PASSWORD", "")
    mqtt_keepalive: int = int(_env("MQTT_KEEPALIVE", "30"))
    # |weight| <= 该值（公斤）视为零读数
    zero_epsilon: float = float(_env("WEIGH_ZERO_EPSILON", "0.0"))


settings = Settings()
