"""FastAPI 路由：扫码、查单、查采样，Swagger 可直接试用。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from .db import session_scope
from .engine import (
    EngineError,
    NoRealTimeZeroError,
    ScaleBusyError,
    get_ticket,
    list_samples,
    list_tickets,
    scan_ticket,
)
from .schemas import SampleOut, ScanResult, TicketOut

router = APIRouter(prefix="/api", tags=["weighbridge"])


def _http_error(exc: EngineError) -> HTTPException:
    if isinstance(exc, (NoRealTimeZeroError, ScaleBusyError)):
        return HTTPException(status_code=409, detail=str(exc))
    return HTTPException(status_code=400, detail=str(exc))


@router.post("/tickets/scan/{transport_code}", response_model=ScanResult)
def scan(transport_code: str):
    """司机扫运输单：空磅以最新实时零读数开单/重扫启用。"""
    with session_scope() as session:
        try:
            result, ticket = scan_ticket(session, transport_code)
        except EngineError as exc:
            raise _http_error(exc)
        # 脱离 session 前把属性物化
        out = TicketOut.model_validate(ticket)
        return ScanResult(result=result, ticket=out)


@router.get("/tickets", response_model=list[TicketOut])
def get_tickets():
    """查单：全部运输单。"""
    with session_scope() as session:
        return [TicketOut.model_validate(t) for t in list_tickets(session)]


@router.get("/tickets/{ticket_id}", response_model=TicketOut)
def get_one_ticket(ticket_id: int):
    with session_scope() as session:
        ticket = get_ticket(session, ticket_id)
        if ticket is None:
            raise HTTPException(status_code=404, detail="单据不存在")
        return TicketOut.model_validate(ticket)


@router.get("/samples", response_model=list[SampleOut])
def get_samples(limit: int = 200):
    """查采样（最新在前）。"""
    with session_scope() as session:
        return [SampleOut.model_validate(s) for s in list_samples(session, limit)]
