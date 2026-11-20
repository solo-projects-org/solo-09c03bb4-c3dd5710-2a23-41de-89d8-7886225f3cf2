"""ORM 模型：采样（samples）与运输单（tickets）。"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base

# 单据状态
TICKET_ACTIVE = "active"        # 已开单，等待稳定正重
TICKET_WAIT_REWEIGH = "reweigh"  # 记重前回零/秤重启，待空磅重扫
TICKET_COMPLETED = "completed"  # 毛重已写入，终态


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Sample(Base):
    __tablename__ = "samples"
    __table_args__ = (
        UniqueConstraint("startup_id", "seq", name="uq_sample_startup_seq"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    startup_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    weight_kg: Mapped[float] = mapped_column(Float, nullable=False)
    stable: Mapped[bool] = mapped_column(Boolean, nullable=False)
    retained: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )


class Ticket(Base):
    __tablename__ = "tickets"
    __table_args__ = (
        # 一个实时零读数最多只能被一张单占用一次
        UniqueConstraint("zero_sample_id", name="uq_ticket_zero_sample"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    transport_code: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=TICKET_ACTIVE)
    zero_sample_id: Mapped[int | None] = mapped_column(
        ForeignKey("samples.id"), nullable=True
    )
    # 状态机已处理到的采样（重启后从该游标继续）
    last_sample_id: Mapped[int | None] = mapped_column(
        ForeignKey("samples.id"), nullable=True
    )
    gross_sample_id: Mapped[int | None] = mapped_column(
        ForeignKey("samples.id"), nullable=True
    )
    gross_weight_kg: Mapped[float | None] = mapped_column(Float, nullable=True)
    note: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    zero_sample: Mapped[Sample | None] = relationship(
        foreign_keys=[zero_sample_id], lazy="joined"
    )
    gross_sample: Mapped[Sample | None] = relationship(
        foreign_keys=[gross_sample_id], lazy="joined"
    )
