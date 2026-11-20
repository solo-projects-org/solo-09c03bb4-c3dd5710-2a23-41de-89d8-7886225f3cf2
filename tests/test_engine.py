"""状态机单元测试：直接驱动 engine，覆盖全部称重规则。"""
from __future__ import annotations

import pytest

from app.db import session_scope
from app.engine import (
    NoRealTimeZeroError,
    ScaleBusyError,
    record_sample,
    resume_after_restart,
    scan_ticket,
)
from app.models import (
    TICKET_ACTIVE,
    TICKET_COMPLETED,
    TICKET_WAIT_REWEIGH,
)


def add_sample(startup, seq, weight, stable, retained=False):
    with session_scope() as s:
        return record_sample(
            s,
            startup_id=startup,
            seq=seq,
            weight_kg=weight,
            stable=stable,
            retained=retained,
        )


def status_of(code):
    with session_scope() as s:
        _, t = scan_lookup(s, code)
        return t


def scan_lookup(s, code):
    from app.engine import get_ticket_by_code

    t = get_ticket_by_code(s, code)
    assert t is not None
    return t.status, t


def test_scan_without_zero_rejected(mem_db):
    with pytest.raises(NoRealTimeZeroError):
        with session_scope() as s:
            scan_ticket(s, "T-001")


def test_retained_zero_is_not_real_time(mem_db):
    # retained 报文必须被丢弃，不能拿来开单
    assert add_sample(1, 0, 0.0, True, retained=True) is None
    with pytest.raises(NoRealTimeZeroError):
        with session_scope() as s:
            scan_ticket(s, "T-001")


def test_happy_path_first_stable_positive(mem_db):
    add_sample(1, 0, 0.0, True)
    with session_scope() as s:
        result, ticket = scan_ticket(s, "T-001")
        assert result == "opened"
        assert ticket.status == TICKET_ACTIVE

    add_sample(1, 1, 3100.0, False)          # 上磅，不稳定
    add_sample(1, 2, 3200.0, True)           # 首个稳定正重
    with session_scope() as s:
        _, t = scan_lookup(s, "T-001")
        assert t.status == TICKET_COMPLETED
        assert t.gross_weight_kg == 3200.0
        assert t.gross_sample_id is not None


def test_zero_reading_single_use(mem_db):
    add_sample(1, 0, 0.0, True)
    with session_scope() as s:
        scan_ticket(s, "T-001")
    # 同一零读数不能开第二张单，且磅被占用
    with pytest.raises(ScaleBusyError):
        with session_scope() as s:
            scan_ticket(s, "T-002")


def test_gap_waits_for_backfill_then_uses_stable(mem_db):
    add_sample(1, 0, 0.0, True)
    with session_scope() as s:
        scan_ticket(s, "T-001")

    add_sample(1, 1, 3100.0, False)
    add_sample(1, 3, 3250.0, True)   # 缺 seq=2，不能先采用
    with session_scope() as s:
        _, t = scan_lookup(s, "T-001")
        assert t.status == TICKET_ACTIVE
        assert t.gross_weight_kg is None

    add_sample(1, 2, 3180.0, False)  # 补报，补齐缺口
    with session_scope() as s:
        _, t = scan_lookup(s, "T-001")
        assert t.status == TICKET_COMPLETED
        # 连续序列里首个稳定正重仍是 seq=3
        assert t.gross_weight_kg == 3250.0


def test_return_to_zero_before_gross_triggers_reweigh(mem_db):
    add_sample(1, 0, 0.0, True)
    with session_scope() as s:
        scan_ticket(s, "T-001")

    add_sample(1, 1, 2900.0, False)
    add_sample(1, 2, 0.0, True)      # 上磅后记重前回零
    with session_scope() as s:
        _, t = scan_lookup(s, "T-001")
        assert t.status == TICKET_WAIT_REWEIGH
        assert "回零" in (t.note or "")

    # 新零读数后原单重扫启用
    add_sample(1, 3, 0.0, True)
    with session_scope() as s:
        result, t = scan_ticket(s, "T-001")
        assert result == "re_armed"
        assert t.status == TICKET_ACTIVE

    add_sample(1, 4, 3300.0, True)
    with session_scope() as s:
        _, t = scan_lookup(s, "T-001")
        assert t.status == TICKET_COMPLETED
        assert t.gross_weight_kg == 3300.0


def test_scale_restart_before_gross_triggers_reweigh(mem_db):
    add_sample(1, 0, 0.0, True)
    with session_scope() as s:
        scan_ticket(s, "T-001")

    add_sample(1, 1, 3000.0, False)
    add_sample(2, 0, 0.0, True)      # 秤重启，启动号变化
    with session_scope() as s:
        _, t = scan_lookup(s, "T-001")
        assert t.status == TICKET_WAIT_REWEIGH
        assert "重启" in (t.note or "")


def test_idle_zeros_before_loading_are_ignored(mem_db):
    add_sample(1, 0, 0.0, True)
    with session_scope() as s:
        scan_ticket(s, "T-001")
    # 司机动作慢，连续多个空磅零读数，不应判成“回零”
    add_sample(1, 1, 0.0, True)
    add_sample(1, 2, 0.0, True)
    add_sample(1, 3, 3200.0, True)
    with session_scope() as s:
        _, t = scan_lookup(s, "T-001")
        assert t.status == TICKET_COMPLETED
        assert t.gross_weight_kg == 3200.0


def test_unstable_positive_keeps_waiting(mem_db):
    add_sample(1, 0, 0.0, True)
    with session_scope() as s:
        scan_ticket(s, "T-001")
    add_sample(1, 1, 3200.0, False)
    add_sample(1, 2, 3210.0, False)
    with session_scope() as s:
        _, t = scan_lookup(s, "T-001")
        assert t.status == TICKET_ACTIVE
    add_sample(1, 3, 3205.0, True)
    with session_scope() as s:
        _, t = scan_lookup(s, "T-001")
        assert t.status == TICKET_COMPLETED
        assert t.gross_weight_kg == 3205.0


def test_one_scale_one_ticket_blocks_second_code(mem_db):
    add_sample(1, 0, 0.0, True)
    with session_scope() as s:
        scan_ticket(s, "T-001")
    with pytest.raises(ScaleBusyError):
        with session_scope() as s:
            scan_ticket(s, "T-002")


def test_completed_rescan_returns_original_gross(mem_db):
    add_sample(1, 0, 0.0, True)
    with session_scope() as s:
        scan_ticket(s, "T-001")
    add_sample(1, 1, 3200.0, True)

    # 再来新的零读数，重扫完成单也必须只回原毛重
    add_sample(1, 2, 0.0, True)
    with session_scope() as s:
        result, t = scan_ticket(s, "T-001")
        assert result == "completed"
        assert t.gross_weight_kg == 3200.0


def test_duplicate_sample_is_idempotent(mem_db):
    add_sample(1, 0, 0.0, True)
    add_sample(1, 0, 0.0, True)  # 重发
    with session_scope() as s:
        from app.models import Sample
        from sqlalchemy import func, select

        count = s.scalar(select(func.count()).select_from(Sample))
        assert count == 1


def test_restart_resumes_from_persisted_state(mem_db, tmp_path):
    """模拟服务重启：换引擎/新会话后，active 单被补报采样接续完成。"""
    add_sample(1, 0, 0.0, True)
    with session_scope() as s:
        scan_ticket(s, "T-001")
    add_sample(1, 1, 3100.0, False)
    add_sample(1, 3, 3200.0, True)   # 缺 seq=2，重启前挂起

    # “重启”：新会话执行接续逻辑
    with session_scope() as s:
        resume_after_restart(s)
        _, t = scan_lookup(s, "T-001")
        assert t.status == TICKET_ACTIVE

    add_sample(1, 2, 3150.0, False)  # 补报到达
    with session_scope() as s:
        resume_after_restart(s)
        _, t = scan_lookup(s, "T-001")
        assert t.status == TICKET_COMPLETED
        assert t.gross_weight_kg == 3200.0


def test_concurrent_scans_only_one_ticket_uses_the_zero(mem_db):
    """并发扫码：库级写锁串行化，只有一张单能占用零读数。"""
    import threading

    add_sample(1, 0, 0.0, True)
    errors: list[Exception] = []
    results: list[str] = []

    def worker(code: str):
        try:
            with session_scope() as s:
                result, _ = scan_ticket(s, code)
                results.append(result)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(f"T-C{i}",)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 1
    assert results == ["opened"]
    assert len(errors) == 4
    assert all(isinstance(e, ScaleBusyError) for e in errors)


def test_interleaved_scan_and_samples_no_cross_ticket(mem_db):
    """扫码与采样交错：单只锚定自己的零读数，后续单不抢毛重。"""
    add_sample(1, 0, 0.0, True)
    with session_scope() as s:
        scan_ticket(s, "T-A")
    add_sample(1, 1, 3200.0, True)   # A 完成
    with session_scope() as s:
        _, a = scan_lookup(s, "T-A")
        assert a.status == TICKET_COMPLETED

    add_sample(1, 2, 0.0, True)      # 新零读数
    with session_scope() as s:
        scan_ticket(s, "T-B")
    add_sample(1, 3, 4100.0, True)   # B 的毛重
    with session_scope() as s:
        _, a = scan_lookup(s, "T-A")
        _, b = scan_lookup(s, "T-B")
        assert a.gross_weight_kg == 3200.0
        assert b.gross_weight_kg == 4100.0
