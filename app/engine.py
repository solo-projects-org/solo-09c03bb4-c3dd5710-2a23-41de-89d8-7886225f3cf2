"""称重业务状态机。

规则汇总：
1. 一台秤一次只称一张单：存在 active/reweigh 单时，新扫码一律拒绝（reweigh
   单在空磅时由原单重扫重新启用）。
2. 开单必须锚定“最新的实时零读数”：retained 报文不入库、零读数全局唯一使用。
3. 自零读数起，在同一次启动的连续采样（缺号先等补报）中寻找首个“稳定正重”。
4. 期间出现非零后的回零 → 待重称；启动号变化（秤重启）→ 待重称。
5. 毛重写入成功后单据为终态，重扫只返回原毛重。
所有推进都在 db_lock 内串行完成，状态全部落 SQLite，重启后可接续。
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import settings
from .db import db_lock
from .models import (
    Sample,
    TICKET_ACTIVE,
    TICKET_COMPLETED,
    TICKET_WAIT_REWEIGH,
    Ticket,
)


class EngineError(Exception):
    """业务错误基类。"""


class NoRealTimeZeroError(EngineError):
    """没有可用的实时零读数。"""


class ScaleBusyError(EngineError):
    """秤上已有未完成单据。"""


def is_zero(weight: float) -> bool:
    return abs(weight) <= settings.zero_epsilon


# ---------------------------------------------------------------- 采样入库
def record_sample(
    session: Session,
    *,
    startup_id: int,
    seq: int,
    weight_kg: float,
    stable: bool,
    retained: bool,
) -> Sample | None:
    """入库一条采样并推进当前活动单。

    retained 报文按“非实时采样”丢弃；重复的 (startup_id, seq) 幂等忽略。
    返回入库的 Sample（被丢弃/重复时返回 None）。
    """
    with db_lock:
        if retained:
            return None

        existing = session.scalar(
            select(Sample).where(
                Sample.startup_id == startup_id, Sample.seq == seq
            )
        )
        if existing is not None:
            return existing

        sample = Sample(
            startup_id=startup_id,
            seq=seq,
            weight_kg=weight_kg,
            stable=stable,
            retained=False,
        )
        session.add(sample)
        session.flush()

        ticket = session.scalar(
            select(Ticket).where(Ticket.status == TICKET_ACTIVE)
        )
        if ticket is not None:
            _advance_ticket(session, ticket)
        return sample


# ---------------------------------------------------------------- 扫码开单
def scan_ticket(session: Session, transport_code: str) -> tuple[str, Ticket]:
    """司机扫码。返回 (result, ticket)。"""
    with db_lock:
        # 1) 同一运输单已有记录：按状态分流
        own = session.scalar(
            select(Ticket).where(Ticket.transport_code == transport_code)
        )
        if own is not None:
            if own.status == TICKET_COMPLETED:
                return "completed", own
            if own.status == TICKET_ACTIVE:
                return "already_active", own
            if own.status == TICKET_WAIT_REWEIGH:
                return _rearm(session, own)

        # 2) 其它单据占用磅秤 → 拒绝，杜绝串单
        busy = session.scalar(
            select(Ticket).where(Ticket.status.in_(
                [TICKET_ACTIVE, TICKET_WAIT_REWEIGH]
            ))
        )
        if busy is not None:
            raise ScaleBusyError(
                f"磅秤被单据 {busy.transport_code} 占用（状态 {busy.status}）"
            )

        # 3) 空磅开单：取最新实时零读数
        zero = _latest_available_zero(session)
        if zero is None:
            raise NoRealTimeZeroError("没有可用的实时零读数，请确认空磅后重新扫码")
        return _open_ticket(session, transport_code, zero)


def _rearm(session: Session, ticket: Ticket) -> tuple[str, Ticket]:
    """待重称单据在空磅时重扫：用新的实时零读数重新启用。"""
    zero = _latest_available_zero(session)
    if zero is None:
        raise NoRealTimeZeroError("没有可用的实时零读数，请确认空磅后重新扫码")
    ticket.zero_sample_id = zero.id
    ticket.last_sample_id = zero.id
    ticket.status = TICKET_ACTIVE
    ticket.note = (
        (ticket.note + "; " if ticket.note else "")
        + "重扫启用，锚定新零读数"
    )
    session.flush()
    # 启用后立即评估一次（零读数之后可能已有补报采样）
    _advance_ticket(session, ticket)
    session.refresh(ticket)
    return "re_armed", ticket


def _open_ticket(
    session: Session, transport_code: str, zero: Sample
) -> tuple[str, Ticket]:
    ticket = Ticket(
        transport_code=transport_code,
        status=TICKET_ACTIVE,
        zero_sample_id=zero.id,
        last_sample_id=zero.id,
    )
    session.add(ticket)
    # 零读数唯一约束兜底；正常路径下“已占用零读数”已被 _latest_available_zero 排除
    session.flush()
    _advance_ticket(session, ticket)
    session.refresh(ticket)
    return "opened", ticket


# ---------------------------------------------------------------- 状态机推进
def _advance_ticket(session: Session, ticket: Ticket) -> None:
    """持续推进单据，直到完成、待重称或等待缺号。

    补报采样的入库 id 一定更大，因此不能按 id 猜“下一条”，而是以
    (startup_id, seq) 为业务游标：每次取本次启动中 seq 大于游标的全部采样，
    按 seq 顺序消费；期望 seq 缺席就等补报，补报入库后下一轮自然补齐。
    游标 last_sample_id 持久化，服务重启后可接续。
    """
    zero = ticket.zero_sample
    assert zero is not None

    while ticket.status == TICKET_ACTIVE:
        # 秤在记重前重启：只要出现更新启动号的任何采样即判定
        newer_startup = session.scalar(
            select(Sample.id)
            .where(Sample.startup_id > zero.startup_id)
            .limit(1)
        )
        if newer_startup is not None:
            trigger = session.get(Sample, newer_startup)
            _mark_reweigh(session, ticket, trigger, "秤在记重前重启（启动号变化）")
            return

        cursor_seq = _cursor_seq(session, ticket)

        pending = session.scalars(
            select(Sample)
            .where(
                Sample.startup_id == zero.startup_id,
                Sample.seq > cursor_seq,
            )
            .order_by(Sample.seq.asc())
        ).all()
        if not pending:
            return  # 等下一条 MQTT

        expected_seq = cursor_seq + 1
        nxt = pending[0]
        if nxt.seq != expected_seq:
            return  # 缺号，先等补报

        ticket.last_sample_id = nxt.id

        if is_zero(nxt.weight_kg):
            # 游标零读数之后已出现过非零采样才算“上磅后回零”
            saw_nonzero = session.scalar(
                select(Sample.id)
                .where(
                    Sample.startup_id == zero.startup_id,
                    Sample.seq > zero.seq,
                    Sample.seq < nxt.seq,
                    Sample.weight_kg > settings.zero_epsilon,
                )
                .limit(1)
            )
            if saw_nonzero is not None:
                _mark_reweigh(session, ticket, nxt, "上磅后记重前回零")
                return
            # 空磅阶段的零读数：忽略，继续等
            continue

        # 非零正重：必须稳定才采用，不稳定继续等
        if nxt.stable and nxt.weight_kg > settings.zero_epsilon:
            ticket.status = TICKET_COMPLETED
            ticket.gross_sample_id = nxt.id
            ticket.gross_weight_kg = nxt.weight_kg
            ticket.completed_at = datetime.now(timezone.utc)
            session.flush()
            return
        # 不稳定的正重：车还在磅上，继续等稳定帧


def _cursor_seq(session: Session, ticket: Ticket) -> int:
    """单据已处理到的采样号（锚零读数之后）。"""
    if ticket.last_sample_id is None:
        return ticket.zero_sample.seq
    last = session.get(Sample, ticket.last_sample_id)
    # 重扫启用后游标一定与锚零读数同启动；防御性处理
    if last is None or last.startup_id != ticket.zero_sample.startup_id:
        return ticket.zero_sample.seq
    return last.seq


def _mark_reweigh(
    session: Session, ticket: Ticket, trigger: Sample, reason: str
) -> None:
    ticket.status = TICKET_WAIT_REWEIGH
    ticket.note = (
        (ticket.note + "; " if ticket.note else "")
        + f"{reason}（触发采样 startup={trigger.startup_id} seq={trigger.seq}）"
    )
    session.flush()


# ---------------------------------------------------------------- 查询辅助
def _latest_available_zero(session: Session) -> Sample | None:
    """最新的、实时的、尚未被任何单据占用的零读数。"""
    used = select(Ticket.zero_sample_id).where(Ticket.zero_sample_id.is_not(None))
    return session.scalar(
        select(Sample)
        .where(
            Sample.retained.is_(False),
            Sample.weight_kg <= settings.zero_epsilon,
            Sample.weight_kg >= -settings.zero_epsilon,
            Sample.id.not_in(used),
        )
        .order_by(Sample.id.desc())
        .limit(1)
    )


def get_ticket(session: Session, ticket_id: int) -> Ticket | None:
    return session.get(Ticket, ticket_id)


def get_ticket_by_code(session: Session, transport_code: str) -> Ticket | None:
    return session.scalar(
        select(Ticket).where(Ticket.transport_code == transport_code)
    )


def list_tickets(session: Session) -> list[Ticket]:
    return list(session.scalars(select(Ticket).order_by(Ticket.id.asc())).all())


def list_samples(session: Session, limit: int = 200) -> list[Sample]:
    return list(
        session.scalars(
            select(Sample).order_by(Sample.id.desc()).limit(limit)
        ).all()
    )


def resume_after_restart(session: Session) -> None:
    """服务重启后：用已落库的采样把仍处于 active 的单继续推进。"""
    with db_lock:
        actives = session.scalars(
            select(Ticket).where(Ticket.status == TICKET_ACTIVE)
        ).all()
        for ticket in actives:
            _advance_ticket(session, ticket)
