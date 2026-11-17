"""数据库连接与进程内日志缓冲。"""

from __future__ import annotations

import os
import threading
from collections import deque
from datetime import datetime, timezone

import psycopg

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://envmon:envmon@localhost:5432/envmon"
)

_lock = threading.Lock()
_logs: deque[dict] = deque(maxlen=500)
_seq = 0


def connect() -> psycopg.Connection:
    """新建独立连接（采集写入演示也用它，模拟独立采集连接）。"""
    return psycopg.connect(
        DATABASE_URL, autocommit=True, options="-c timezone=UTC"
    )


def log(msg: str):
    global _seq
    with _lock:
        _seq += 1
        _logs.append(
            {
                "seq": _seq,
                "at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "msg": msg,
            }
        )
    print(f"[log] {msg}", flush=True)


def logs_since(seq: int) -> list[dict]:
    with _lock:
        return [e for e in _logs if e["seq"] > seq]


def ensure_meta_tables():
    """元数据表兜底创建（init.sql 已建则跳过）。"""
    with connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_runs (
                month           date        PRIMARY KEY,
                status          text        NOT NULL DEFAULT 'preparing',
                total_estimate  bigint      NOT NULL DEFAULT 0,
                copied_rows     bigint      NOT NULL DEFAULT 0,
                applied_delta   bigint      NOT NULL DEFAULT 0,
                last_station    text,
                last_ts         timestamptz,
                error           text,
                updated_at      timestamptz NOT NULL DEFAULT now(),
                created_at      timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_delta (
                seq          bigserial PRIMARY KEY,
                month        date        NOT NULL,
                op           text        NOT NULL,
                old_station  text,
                old_ts       timestamptz,
                new_station  text,
                new_ts       timestamptz,
                value        integer
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS migration_delta_month_seq "
            "ON migration_delta (month, seq)"
        )
        _upgrade_delta_schema(conn)


def _upgrade_delta_schema(conn):
    """旧版库（单键 station_id/ts）平滑升级为 OLD/NEW 双键结构。

    升级只在没有迁移进行时安全：旧结构无法表达改键，存在进行中迁移的
    旧库不应出现；此处仅在列缺失时按"OLD=NEW=旧键"回填，历史语义保持不变。
    """
    cols = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'migration_delta'"
        )
    }
    if "new_station" in cols:
        return
    conn.execute("ALTER TABLE migration_delta ADD COLUMN old_station text")
    conn.execute("ALTER TABLE migration_delta ADD COLUMN old_ts timestamptz")
    conn.execute("ALTER TABLE migration_delta ADD COLUMN new_station text")
    conn.execute("ALTER TABLE migration_delta ADD COLUMN new_ts timestamptz")
    conn.execute(
        "UPDATE migration_delta SET "
        "old_station = station_id, old_ts = ts,"
        "new_station = station_id, new_ts = ts "
        "WHERE station_id IS NOT NULL"
    )
    conn.execute("ALTER TABLE migration_delta DROP COLUMN station_id")
    conn.execute("ALTER TABLE migration_delta DROP COLUMN ts")
