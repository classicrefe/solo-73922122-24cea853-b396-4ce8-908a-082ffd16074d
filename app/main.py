"""FastAPI 入口：迁移控制、进度查询、父表查询、采集写入演示、一致性校验。"""

from __future__ import annotations

import os
import random
import threading
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

import migrator
from db import connect, ensure_meta_tables, log, logs_since
from migrator import (
    AlreadyDone,
    MigrationBusy,
    current_worker_month,
    next_month,
    parse_month,
    start_migration,
)

app = FastAPI(title="观测数据月分区在线迁移")
STATIC = Path(__file__).parent / "static"


@app.on_event("startup")
def _startup():
    ensure_meta_tables()
    log("应用启动，元数据表就绪")


# --------------------------------------------------------------------- 页面
@app.get("/", response_class=FileResponse)
def index():
    return STATIC / "index.html"


# --------------------------------------------------------------------- 状态
def _partition_months(conn) -> dict[str, str]:
    """已挂载的月分区：{ 'YYYY-MM': 'observations_YYYY_MM' }（默认分区除外）。"""
    rows = conn.execute(
        """
        SELECT c.relname FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        WHERE i.inhparent = 'observations'::regclass
          AND c.relname <> 'observations_default'
        """
    ).fetchall()
    out = {}
    for (name,) in rows:
        # observations_2026_01 -> 2026-01
        try:
            suffix = name.rsplit("observations_", 1)[1]
            y, m = suffix.split("_")
            out[f"{y}-{m}"] = name
        except Exception:  # noqa: BLE001
            out[name] = name
    return out


@app.get("/api/state")
def api_state():
    with connect() as conn:
        default_rows = {
            r[0].isoformat()[:7]: r[1]
            for r in conn.execute(
                "SELECT (date_trunc('month', ts AT TIME ZONE 'UTC'))::date AS m,"
                "       count(*) FROM observations_default GROUP BY 1"
            )
        }
        parts = _partition_months(conn)
        part_counts = {}
        for ym, tbl in parts.items():
            part_counts[ym] = conn.execute(
                f'SELECT count(*) FROM "{tbl}"'
            ).fetchone()[0]
        runs = {
            r[0].isoformat()[:7]: {
                "status": r[1],
                "total_estimate": r[2],
                "copied_rows": r[3],
                "applied_delta": r[4],
                "error": r[5],
                "updated_at": r[6].isoformat() if r[6] else None,
            }
            for r in conn.execute(
                "SELECT month, status, total_estimate, copied_rows,"
                "       applied_delta, error, updated_at FROM migration_runs"
            )
        }
        backlog = {
            r[0].isoformat()[:7]: r[1]
            for r in conn.execute(
                "SELECT month, count(*) FROM migration_delta GROUP BY 1"
            )
        }
    months = sorted(set(default_rows) | set(parts) | set(runs), reverse=True)
    return {
        "months": [
            {
                "month": ym,
                "default_rows": default_rows.get(ym, 0),
                "partition": parts.get(ym),
                "partition_rows": part_counts.get(ym, 0),
                "run": runs.get(ym),
                "delta_backlog": backlog.get(ym, 0),
            }
            for ym in months
        ],
        "worker_month": current_worker_month(),
        "auto_writer": auto_writer_state(),
    }


@app.get("/api/logs")
def api_logs(since: int = 0):
    return {"logs": logs_since(since)}


# --------------------------------------------------------------------- 迁移
class MigrateReq(BaseModel):
    month: str
    batch_size: int = Field(default=500, ge=1, le=10000)
    sleep_ms: int = Field(default=0, ge=0, le=10000)
    fail_at: str | None = None  # 故障注入（演示/自测用）


@app.post("/api/migrate")
def api_migrate(req: MigrateReq):
    try:
        return start_migration(req.month, req.batch_size, req.sleep_ms, req.fail_at)
    except AlreadyDone as e:
        raise HTTPException(status_code=409, detail=str(e))
    except MigrationBusy as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ------------------------------------------------------------------ 父表查询
@app.get("/api/query")
def api_query(month: str, limit: int = 20):
    m = parse_month(month)
    lo, hi = m.isoformat(), next_month(m).isoformat()
    with connect() as conn:
        cnt, distinct_cnt, vsum = conn.execute(
            "SELECT count(*), count(DISTINCT (station_id, ts)),"
            "       coalesce(sum(value), 0)"
            " FROM observations WHERE ts >= %s::date AND ts < %s::date",
            (lo, hi),
        ).fetchone()
        rows = conn.execute(
            "SELECT station_id, ts, value FROM observations"
            " WHERE ts >= %s::date AND ts < %s::date"
            " ORDER BY ts DESC, station_id LIMIT %s",
            (lo, hi, min(limit, 200)),
        ).fetchall()
    return {
        "month": month,
        "parent_count": cnt,
        "distinct_keys": distinct_cnt,
        "duplicates": cnt - distinct_cnt,
        "value_sum": vsum,
        "sample": [
            {"station_id": r[0], "ts": r[1].isoformat(), "value": r[2]}
            for r in rows
        ],
    }


# --------------------------------------------------------------- 采集写入演示
STATIONS = [f"ST{i:03d}" for i in range(1, 9)]


def _random_ts_in_month(conn, lo: str, hi: str):
    """月内随机时刻，返回带时区的 datetime（psycopg 可直接绑定到 timestamptz）。"""
    return conn.execute(
        "SELECT (%s::timestamptz + random() * (%s::timestamptz - %s::timestamptz))"
        "::timestamptz",
        (lo, hi, lo),
    ).fetchone()[0]


def _random_row(conn, lo: str, hi: str):
    return conn.execute(
        "SELECT station_id, ts, value FROM observations"
        " WHERE ts >= %s::date AND ts < %s::date"
        " ORDER BY random() LIMIT 1",
        (lo, hi),
    ).fetchone()


def _free_station(conn, ts, current: str) -> str | None:
    """找一个在 ts 时刻尚未占用的其他站号（供改站号更正使用）。"""
    for s in random.sample(STATIONS, len(STATIONS)):
        if s == current:
            continue
        hit = conn.execute(
            "SELECT 1 FROM observations WHERE station_id = %s AND ts = %s",
            (s, ts),
        ).fetchone()
        if not hit:
            return s
    return None


def _free_ts(conn, lo: str, hi: str, station: str, old_ts):
    """在目标月内为该站号找一个未占用的随机时刻。"""
    for _ in range(12):
        ts = _random_ts_in_month(conn, lo, hi)
        if ts == old_ts:
            continue
        hit = conn.execute(
            "SELECT 1 FROM observations WHERE station_id = %s AND ts = %s",
            (station, ts),
        ).fetchone()
        if not hit:
            return ts
    return None


def demo_write(op: str, month: str) -> dict:
    """模拟独立采集连接：每次新建连接，对父表直接 INSERT/UPDATE/DELETE/改键。"""
    m = parse_month(month)
    lo, hi = m.isoformat(), next_month(m).isoformat()
    with connect() as conn:  # 独立连接，自动提交
        if op == "insert":
            for _ in range(10):
                station = random.choice(STATIONS)
                ts = _random_ts_in_month(conn, lo, hi)
                value = random.randint(0, 100)
                row = conn.execute(
                    "INSERT INTO observations (station_id, ts, value)"
                    " VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
                    " RETURNING station_id, ts, value",
                    (station, ts, value),
                ).fetchone()
                if row:
                    return {"op": "insert", "station_id": row[0],
                            "ts": row[1].isoformat(), "value": row[2]}
            return {"op": "insert", "skipped": "随机主键冲突，未写入"}
        # 改站号更正：UPDATE 主键的 station_id 部分（采集端用独立 SQL 更正站号）
        if op == "rekey_station":
            # 优先选"该时刻尚有空闲站号"的行，避免与同刻其他行主键冲突
            row = conn.execute(
                "SELECT station_id, ts, value FROM observations"
                " WHERE ts >= %s::date AND ts < %s::date"
                "   AND ts IN (SELECT ts FROM observations"
                "               WHERE ts >= %s::date AND ts < %s::date"
                "               GROUP BY ts HAVING count(*) < %s)"
                " ORDER BY random() LIMIT 1",
                (lo, hi, lo, hi, len(STATIONS)),
            ).fetchone()
            if not row:
                return {"op": op, "skipped": "该月各时刻站号均已占满，无空位可改"}
            old_station, ts, old_value = row
            new_station = _free_station(conn, ts, old_station)
            if not new_station:
                return {"op": op, "skipped": "该时刻所有站号均被占用"}
            conn.execute(
                "UPDATE observations SET station_id = %s"
                " WHERE station_id = %s AND ts = %s",
                (new_station, old_station, ts),
            )
            return {"op": op, "ts": ts.isoformat(),
                    "old_station_id": old_station, "station_id": new_station,
                    "value": old_value}
        # 改同月时刻更正：UPDATE 主键的 ts 部分，仍在本月
        if op == "rekey_ts":
            row = _random_row(conn, lo, hi)
            if not row:
                return {"op": op, "skipped": "该月没有可操作的数据"}
            station, old_ts, old_value = row
            new_ts = _free_ts(conn, lo, hi, station, old_ts)
            if not new_ts:
                return {"op": op, "skipped": "找不到空闲时刻"}
            conn.execute(
                "UPDATE observations SET ts = %s"
                " WHERE station_id = %s AND ts = %s",
                (new_ts, station, old_ts),
            )
            return {"op": op, "station_id": station,
                    "old_ts": old_ts.isoformat(), "ts": new_ts.isoformat(),
                    "value": old_value}
        # 跨月时刻更正：把一条观测移动到相邻/其他月份
        if op == "move_month":
            row = _random_row(conn, lo, hi)
            if not row:
                return {"op": op, "skipped": "该月没有可操作的数据"}
            station, old_ts, old_value = row
            target = conn.execute(
                "SELECT (date_trunc('month', ts AT TIME ZONE 'UTC'))::date"
                " FROM observations GROUP BY 1"
                " HAVING (date_trunc('month', ts AT TIME ZONE 'UTC'))::date <> %s"
                " ORDER BY random() LIMIT 1",
                (m,),
            ).fetchone()
            if not target:
                return {"op": op, "skipped": "没有其他月份可移动"}
            tm = target[0]
            tlo, thi = tm.isoformat(), next_month(tm).isoformat()
            new_ts = _free_ts(conn, tlo, thi, station, old_ts)
            if not new_ts:
                return {"op": op, "skipped": "目标月份找不到空闲时刻"}
            conn.execute(
                "UPDATE observations SET ts = %s"
                " WHERE station_id = %s AND ts = %s",
                (new_ts, station, old_ts),
            )
            return {"op": op, "station_id": station,
                    "old_ts": old_ts.isoformat(), "ts": new_ts.isoformat(),
                    "from_month": month, "to_month": tm.isoformat()[:7],
                    "value": old_value}
        # update / delete 先随机取该月一行
        row = _random_row(conn, lo, hi)
        if not row:
            return {"op": op, "skipped": "该月没有可操作的数据"}
        if op == "update":
            new_value = random.randint(0, 100)
            conn.execute(
                "UPDATE observations SET value = %s"
                " WHERE station_id = %s AND ts = %s",
                (new_value, row[0], row[1]),
            )
            return {"op": "update", "station_id": row[0], "ts": row[1].isoformat(),
                    "old_value": row[2], "value": new_value}
        if op == "delete":
            conn.execute(
                "DELETE FROM observations WHERE station_id = %s AND ts = %s",
                (row[0], row[1]),
            )
            return {"op": "delete", "station_id": row[0], "ts": row[1].isoformat(),
                    "old_value": row[2]}
    raise HTTPException(status_code=400, detail=f"未知操作: {op}")


class WriteReq(BaseModel):
    op: str
    month: str


@app.post("/api/demo/write")
def api_demo_write(req: WriteReq):
    result = demo_write(req.op, req.month)
    log(f"[采集写入] {result}")
    return result


# 自动采集写入（演示复制期间持续不断的独立写入）
class AutoReq(BaseModel):
    enabled: bool
    month: str
    interval_ms: int = Field(default=300, ge=50, le=10000)


_auto_stop = threading.Event()
_auto_thread: threading.Thread | None = None
_auto_lock = threading.Lock()


def _auto_loop(month: str, interval_ms: int):
    # 含改主键类更正（改站号/改同月时刻/跨月移动），持续演练增量捕获的改键路径
    ops = (
        ["insert"] * 4 + ["update"] * 3 + ["delete"] * 2
        + ["rekey_station", "rekey_ts", "move_month"]
    )
    while not _auto_stop.is_set():
        try:
            result = demo_write(random.choice(ops), month)
            log(f"[自动采集] {result}")
        except Exception as exc:  # noqa: BLE001
            log(f"[自动采集] 写入失败: {exc!r}")
        _auto_stop.wait(interval_ms / 1000)


def auto_writer_state() -> dict:
    t = _auto_thread
    return {"running": bool(t and t.is_alive())}


@app.post("/api/demo/auto")
def api_demo_auto(req: AutoReq):
    global _auto_thread
    with _auto_lock:
        if _auto_thread and _auto_thread.is_alive():
            _auto_stop.set()
            _auto_thread.join(timeout=3)
        _auto_thread = None
        if req.enabled:
            parse_month(req.month)
            _auto_stop.clear()
            _auto_thread = threading.Thread(
                target=_auto_loop, args=(req.month, req.interval_ms),
                daemon=True, name="auto-writer",
            )
            _auto_thread.start()
            log(f"[自动采集] 已启动，目标月份 {req.month}，间隔 {req.interval_ms}ms")
        else:
            log("[自动采集] 已停止")
    return auto_writer_state()


# ------------------------------------------------------------- 自测用崩溃注入
@app.post("/api/debug/crash")
def api_debug_crash():
    """模拟进程宕机：0.2s 后 os._exit，不做任何清理（自测断点续迁用）。"""
    log("[debug] 收到崩溃指令，进程即将硬退出（模拟宕机）")
    threading.Timer(0.2, lambda: os._exit(1)).start()
    return {"crashing": True}


# --------------------------------------------------------------------- 校验
@app.get("/api/verify")
def api_verify(month: str):
    m = parse_month(month)
    lo, hi = m.isoformat(), next_month(m).isoformat()
    with connect() as conn:
        parent_cnt, parent_distinct, parent_sum = conn.execute(
            "SELECT count(*), count(DISTINCT (station_id, ts)),"
            "       coalesce(sum(value),0) FROM observations"
            " WHERE ts >= %s::date AND ts < %s::date",
            (lo, hi),
        ).fetchone()
        default_cnt = conn.execute(
            "SELECT count(*) FROM observations_default"
            " WHERE ts >= %s::date AND ts < %s::date",
            (lo, hi),
        ).fetchone()[0]
        parts = _partition_months(conn)
        part_cnt = None
        if month in parts:
            part_cnt = conn.execute(
                f'SELECT count(*) FROM "{parts[month]}"'
            ).fetchone()[0]
        run = conn.execute(
            "SELECT status FROM migration_runs WHERE month = %s", (m,)
        ).fetchone()
    return {
        "month": month,
        "status": run[0] if run else None,
        "parent_count": parent_cnt,
        "parent_duplicates": parent_cnt - parent_distinct,
        "parent_value_sum": parent_sum,
        "default_remaining": default_cnt,
        "partition": parts.get(month),
        "partition_count": part_cnt,
        "consistent": (
            parent_cnt == parent_distinct
            and (part_cnt is None or part_cnt == parent_cnt)
            and (part_cnt is None or default_cnt == 0)
        ),
    }
