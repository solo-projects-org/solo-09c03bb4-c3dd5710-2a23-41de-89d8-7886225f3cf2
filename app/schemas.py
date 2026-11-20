"""Pydantic 模型：MQTT 报文与 HTTP 响应。"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class ScaleSampleIn(BaseModel):
    """模拟秤上报报文（MQTT payload）。"""

    startup_id: int = Field(ge=0, description="启动号，每次开机递增")
    seq: int = Field(ge=0, description="本次启动内的连续采样号")
    weight_kg: float = Field(description="公斤数")
    stable: bool = Field(description="稳定标记")


class SampleOut(BaseModel):
    id: int
    startup_id: int
    seq: int
    weight_kg: float
    stable: bool
    retained: bool
    received_at: datetime

    model_config = {"from_attributes": True}


class TicketOut(BaseModel):
    id: int
    transport_code: str
    status: str
    zero_sample_id: int | None
    gross_sample_id: int | None
    gross_weight_kg: float | None
    note: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None

    model_config = {"from_attributes": True}


class ScanResult(BaseModel):
    result: str = Field(description="opened / re_armed / waiting_reweigh / completed / already_active")
    ticket: TicketOut
