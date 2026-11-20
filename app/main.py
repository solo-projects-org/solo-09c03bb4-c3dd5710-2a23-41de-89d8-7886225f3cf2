"""FastAPI 入口：启动时建库/接续状态，连接 MQTT；关闭时断开。"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .db import init_db, session_scope
from .engine import resume_after_restart
from .routers import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("weighbridge")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    with session_scope() as session:
        resume_after_restart(session)
    logger.info("启动接续检查完成")

    mqtt_client = None
    if os.environ.get("DISABLE_MQTT") != "1":
        from .mqtt_client import get_mqtt_client

        mqtt_client = get_mqtt_client()
        mqtt_client.start()
    else:
        logger.warning("MQTT 已禁用（DISABLE_MQTT=1）")
    try:
        yield
    finally:
        if mqtt_client is not None:
            mqtt_client.stop()


app = FastAPI(
    title="单磅地磅采集 API",
    description=(
        "送粮车毛重采集：扫运输单开单，MQTT QoS1 接收模拟秤采样，"
        "稳定正重写入毛重。文档同页支持扫码、查单、查采样。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(router)


@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok"}
