"""SQLite 引擎与会话工厂。

SQLite 单文件、单写入者：开启 WAL 并对写操作加库级互斥锁，
保证 MQTT 回调线程与 HTTP 线程并发时状态机的串行推进。
引擎采用懒加载，测试可通过 reinit_engine() 切换为内存库。
"""
from __future__ import annotations

import os
import threading
from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import StaticPool

from .config import settings


class Base(DeclarativeBase):
    pass


db_lock = threading.RLock()

_engine = None
SessionLocal = None


def _build_engine():
    if settings.db_path.startswith("sqlite://"):
        url = settings.db_path
    else:
        path = settings.db_path
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        url = f"sqlite:///{path}"
    in_memory = url.endswith(":memory:")
    engine = create_engine(
        url,
        connect_args={"check_same_thread": False, "timeout": 30},
        future=True,
        poolclass=StaticPool if in_memory else None,
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _record):  # noqa: ANN001
        cur = dbapi_conn.cursor()
        if not in_memory:
            cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA foreign_keys=ON")
        cur.execute("PRAGMA busy_timeout=30000")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.close()

    return engine


def reinit_engine():
    """丢弃当前引擎并按当前配置重建（测试切换内存库时使用）。"""
    global _engine, SessionLocal
    if _engine is not None:
        _engine.dispose()
    _engine = _build_engine()
    SessionLocal = sessionmaker(bind=_engine, autoflush=False, future=True)
    return _engine


def get_engine():
    if _engine is None:
        reinit_engine()
    return _engine


class _EngineProxy:
    def __getattr__(self, name):
        return getattr(get_engine(), name)


# 让 `from .db import engine` 拿到始终指向当前引擎的代理
engine = _EngineProxy()


def init_db() -> None:
    # 导入模型模块以触发 ORM 类注册，再建表
    from . import models as _models  # pyflakes: 注册副作用

    Base.metadata.create_all(bind=get_engine())
    return _models


@contextmanager
def session_scope() -> Session:
    if SessionLocal is None:
        reinit_engine()
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
