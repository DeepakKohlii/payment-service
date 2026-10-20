import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

from sqlalchemy import (
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    func,
    text,
)
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from app.config import settings

log = logging.getLogger("db")
T = TypeVar("T")

metadata = MetaData()

transactions = Table(
    "transactions",
    metadata,
    # Primary key on transaction_id is what makes webhooks idempotent:
    # the database itself refuses a second row for the same id.
    Column("transaction_id", String(128), primary_key=True),
    Column("source_account", String(128), nullable=False),
    Column("destination_account", String(128), nullable=False),
    Column("amount", Numeric(18, 2), nullable=False),
    Column("currency", String(3), nullable=False),
    Column("status", String(16), nullable=False, server_default="PROCESSING"),
    Column("attempts", Integer, nullable=False, server_default="0"),
    # Worker lease: while locked_until is in the future, another worker won't claim it.
    Column("locked_until", DateTime(timezone=True), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("processed_at", DateTime(timezone=True), nullable=True),
)

Index(
    "ix_transactions_pending",
    transactions.c.created_at,
    postgresql_where=text("status = 'PROCESSING'"),
)


def _build_engine(pool_size: int, pool_recycle: int) -> AsyncEngine:
    url = make_url(settings.database_url)
    if url.drivername in ("postgres", "postgresql"):
        url = url.set(drivername="postgresql+asyncpg")

    connect_args: dict = {}
    sslmode = url.query.get("sslmode")
    if sslmode:
        url = url.difference_update_query(["sslmode"])
        if sslmode != "disable":
            connect_args["ssl"] = "require"

    return create_async_engine(
        url,
        pool_size=pool_size,
        max_overflow=0, 
        pool_timeout=10,
        pool_pre_ping=False,
        pool_recycle=pool_recycle,
        connect_args=connect_args,
    ).execution_options(isolation_level="AUTOCOMMIT")


ingest_engine = _build_engine(settings.db_pool_size_ingest, pool_recycle=-1)

engine = _build_engine(settings.db_pool_size_general, pool_recycle=240)


async def with_retry(eng: AsyncEngine, fn: Callable[[AsyncConnection], Awaitable[T]]) -> T:
    """Run fn on a pooled connection, retrying once on a fresh connection if the pooled
    one turned out to be dead. Only used for idempotent statements, so a retry is safe."""
    for attempt in (1, 2):
        try:
            async with eng.connect() as conn:
                return await fn(conn)
        except DBAPIError as exc:
            if attempt == 1 and exc.connection_invalidated:
                log.warning("stale DB connection, retrying on a fresh one")
                continue
            raise
    raise AssertionError("unreachable")


async def init_db() -> None:
    async with engine.connect() as conn:
        await conn.run_sync(metadata.create_all)


async def dispose() -> None:
    await ingest_engine.dispose()
    await engine.dispose()
