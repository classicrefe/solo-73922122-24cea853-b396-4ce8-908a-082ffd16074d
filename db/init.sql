-- ============================================================================
-- 环境监测数仓 · 观测表分区迁移演示 — 数据库初始化
-- ----------------------------------------------------------------------------
--  * observations  : 父表，按 UTC 时刻 RANGE 分区，主键 (station_id, ts)
--  * observations_default : 唯一的默认分区，历史观测最初全部落在这里
--  * migration_runs  : 每个月份迁移的状态机记录（幂等/断点续迁的依据）
--  * migration_delta : 迁移期间由触发器捕获的增量变更（新增/更正/删除）
-- ============================================================================

CREATE TABLE IF NOT EXISTS observations (
    station_id  text          NOT NULL,
    ts          timestamptz   NOT NULL,
    value       integer       NOT NULL,
    PRIMARY KEY (station_id, ts)
) PARTITION BY RANGE (ts);

CREATE TABLE IF NOT EXISTS observations_default
    PARTITION OF observations DEFAULT;

-- 迁移状态机：idle → preparing → copying → applying → finalizing → done
-- failed 状态仅作展示；重试时从断点继续（影子表/增量表均保留）。
CREATE TABLE IF NOT EXISTS migration_runs (
    month           date        PRIMARY KEY,   -- 月份首日（UTC）
    status          text        NOT NULL DEFAULT 'preparing',
    total_estimate  bigint      NOT NULL DEFAULT 0,
    copied_rows     bigint      NOT NULL DEFAULT 0,
    applied_delta   bigint      NOT NULL DEFAULT 0,
    last_station    text,
    last_ts         timestamptz,
    error           text,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- 增量变更日志：迁移期间默认分区上该月数据的 INSERT/UPDATE/DELETE。
-- 关键点：采集端可能"更正观测身份"（改站号、改同月时刻、跨月移动时刻），
-- 即 UPDATE 会改变主键 (station_id, ts)。因此每条变更同时记录 OLD/NEW 两套键：
--   insert : OLD 为空，NEW 为新行
--   delete : OLD 为被删行，NEW 为空
--   update : OLD/NEW 都可能存在；同月改键时两者不同，跨月移动时其一落在月外而为空
-- value 始终取 NEW.value（delete 时为 NULL）。
CREATE TABLE IF NOT EXISTS migration_delta (
    seq          bigserial PRIMARY KEY,
    month        date        NOT NULL,
    op           text        NOT NULL CHECK (op IN ('insert','update','delete')),
    old_station  text,
    old_ts       timestamptz,
    new_station  text,
    new_ts       timestamptz,
    value        integer
);
CREATE INDEX IF NOT EXISTS migration_delta_month_seq
    ON migration_delta (month, seq);

-- ============================================================================
-- 演示种子数据：最近 6 个自然月（UTC），8 个站点，每小时一条整数测值。
-- 全部写入默认分区 —— 即"历史观测 initially 都在默认分区"。
-- ============================================================================
INSERT INTO observations (station_id, ts, value)
SELECT 'ST' || lpad(s::text, 3, '0'),
       date_trunc('hour', t),
       (20 + 60 * random())::int
FROM generate_series(1, 8) AS s
CROSS JOIN generate_series(
        date_trunc('month', now()) - interval '5 months',
        now() - interval '1 hour',
        interval '1 hour'
    ) AS t
ON CONFLICT (station_id, ts) DO NOTHING;
