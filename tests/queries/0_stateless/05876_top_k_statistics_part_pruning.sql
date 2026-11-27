-- Random settings limits: merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=(0, 0)
-- Part pruning for `ORDER BY ... LIMIT` using `basic` column statistics.

SET explain_query_plan_default = 'legacy';
SET min_marks_per_part_to_load_statistics = 0;
SET use_statistics_for_part_pruning = 1;
SET materialize_statistics_on_insert = 1;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET use_query_condition_cache = 0;
-- CI randomizes `automatic_parallel_replicas_mode` to 2, which looks up a `parallel_replicas` cluster.
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET allow_suspicious_low_cardinality_types = 1;

-- Direction, `OFFSET`, `Decimal`, `DateTime`, and the setting turned off.
DROP TABLE IF EXISTS t_topk_stats;
CREATE TABLE t_topk_stats
(
    p UInt8,
    id UInt64,
    v Int64,
    d Decimal64(2),
    dt DateTime('UTC')
)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0;

-- p=0: [300..309], p=1: [100..109], p=2: [400..409], p=3: [0..9], p=4: [200..209]
INSERT INTO t_topk_stats SELECT 0, number, 300 + number, toDecimal64(300 + number, 2), toDateTime('2025-01-01 00:00:00', 'UTC') + 300 + number FROM numbers(10);
INSERT INTO t_topk_stats SELECT 1, number + 10, 100 + number, toDecimal64(100 + number, 2), toDateTime('2025-01-01 00:00:00', 'UTC') + 100 + number FROM numbers(10);
INSERT INTO t_topk_stats SELECT 2, number + 20, 400 + number, toDecimal64(400 + number, 2), toDateTime('2025-01-01 00:00:00', 'UTC') + 400 + number FROM numbers(10);
INSERT INTO t_topk_stats SELECT 3, number + 30, number, toDecimal64(number, 2), toDateTime('2025-01-01 00:00:00', 'UTC') + number FROM numbers(10);
INSERT INTO t_topk_stats SELECT 4, number + 40, 200 + number, toDecimal64(200 + number, 2), toDateTime('2025-01-01 00:00:00', 'UTC') + 200 + number FROM numbers(10);

SELECT '-- ASC / DESC / OFFSET / Decimal / DateTime';
SELECT countIf(explain LIKE '%Description: TopK%') > 0, countIf(explain LIKE '%Parts: 1/5%') > 0
FROM (EXPLAIN indexes = 1 SELECT v FROM t_topk_stats ORDER BY v ASC LIMIT 5);
SELECT v FROM t_topk_stats ORDER BY v ASC LIMIT 5;

SELECT countIf(explain LIKE '%Description: TopK%') > 0, countIf(explain LIKE '%Parts: 1/5%') > 0
FROM (EXPLAIN indexes = 1 SELECT v FROM t_topk_stats ORDER BY v DESC LIMIT 5);
SELECT v FROM t_topk_stats ORDER BY v DESC LIMIT 5;

SELECT countIf(explain LIKE '%Description: TopK%') > 0, countIf(explain LIKE '%Parts: 2/5%') > 0
FROM (EXPLAIN indexes = 1 SELECT v FROM t_topk_stats ORDER BY v ASC LIMIT 5 OFFSET 10);
SELECT v FROM t_topk_stats ORDER BY v ASC LIMIT 5 OFFSET 10;

SELECT countIf(explain LIKE '%Description: TopK%') > 0, countIf(explain LIKE '%Parts: 1/5%') > 0
FROM (EXPLAIN indexes = 1 SELECT d FROM t_topk_stats ORDER BY d DESC LIMIT 3);
SELECT d FROM t_topk_stats ORDER BY d DESC LIMIT 3;

SELECT countIf(explain LIKE '%Description: TopK%') > 0, countIf(explain LIKE '%Parts: 1/5%') > 0
FROM (EXPLAIN indexes = 1 SELECT dt FROM t_topk_stats ORDER BY dt ASC LIMIT 3);
SELECT dt FROM t_topk_stats ORDER BY dt ASC LIMIT 3;

SELECT countIf(explain LIKE '%Description: TopK%')
FROM (EXPLAIN indexes = 1 SELECT v FROM t_topk_stats ORDER BY v ASC LIMIT 5 SETTINGS use_statistics_for_part_pruning = 0);
DROP TABLE t_topk_stats;

-- A tie on the first key must keep every part that can win on a later key.
DROP TABLE IF EXISTS t_topk_ties;
CREATE TABLE t_topk_ties (p UInt8, id UInt64, v Int64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_topk_ties SELECT 0, number, 10 FROM numbers(5);
INSERT INTO t_topk_ties SELECT 1, 100 + number, 10 FROM numbers(5);
INSERT INTO t_topk_ties SELECT 2, 200 + number, 10 FROM numbers(5);
INSERT INTO t_topk_ties SELECT 3, 300 + number, 20 + number FROM numbers(5);

SELECT '-- Ties';
SELECT countIf(explain LIKE '%Description: TopK%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT v, id FROM t_topk_ties ORDER BY v ASC, id DESC LIMIT 3);
SELECT v, id FROM t_topk_ties ORDER BY v ASC, id DESC LIMIT 3;
DROP TABLE t_topk_ties;

-- `Nullable` and `LowCardinality(Nullable)`, both NULLS orders.
DROP TABLE IF EXISTS t_topk_null;
CREATE TABLE t_topk_null (p UInt8, id UInt64, v Nullable(Int64), lc LowCardinality(Nullable(Int64)))
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_topk_null SELECT 0, number, NULL, NULL FROM numbers(4);
INSERT INTO t_topk_null VALUES (1, 10, NULL, NULL), (1, 11, NULL, NULL), (1, 12, 100, 100), (1, 13, 101, 101);
INSERT INTO t_topk_null SELECT 2, 20 + number, 10 + number, 10 + number FROM numbers(4);
INSERT INTO t_topk_null SELECT 3, 30 + number, 200 + number, 200 + number FROM numbers(4);

SELECT '-- Nullable / LowCardinality(Nullable)';
SELECT countIf(explain LIKE '%Description: TopK%') > 0, countIf(explain LIKE '%Parts: 1/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT v FROM t_topk_null ORDER BY v ASC NULLS LAST LIMIT 3);
SELECT v FROM t_topk_null ORDER BY v ASC NULLS LAST LIMIT 3;

SELECT countIf(explain LIKE '%Description: TopK%') > 0, countIf(explain LIKE '%Parts: 1/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT v FROM t_topk_null ORDER BY v DESC NULLS LAST LIMIT 3);
SELECT v FROM t_topk_null ORDER BY v DESC NULLS LAST LIMIT 3;

SELECT countIf(explain LIKE '%Description: TopK%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT v FROM t_topk_null ORDER BY v ASC NULLS FIRST LIMIT 5);
SELECT v FROM t_topk_null ORDER BY v ASC NULLS FIRST LIMIT 5;

SELECT countIf(explain LIKE '%Description: TopK%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT v FROM t_topk_null ORDER BY v ASC NULLS FIRST LIMIT 8);
SELECT v FROM t_topk_null ORDER BY v ASC NULLS FIRST LIMIT 8;

SELECT countIf(explain LIKE '%Description: TopK%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT lc FROM t_topk_null ORDER BY lc DESC NULLS FIRST LIMIT 8);
SELECT lc FROM t_topk_null ORDER BY lc DESC NULLS FIRST LIMIT 8;
DROP TABLE t_topk_null;

-- `NaN` is outside min/max, so row-count pruning must not run.
DROP TABLE IF EXISTS t_topk_nan;
CREATE TABLE t_topk_nan (p UInt8, id UInt64, f Float64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0;

-- p=0: one finite 1.0 and four `NaN` (`estimated_max` = 1.0); p=1: [100, 104]; p=2: all `NaN`.
INSERT INTO t_topk_nan SELECT 0, number, number = 0 ? 1.0 : nan FROM numbers(5);
INSERT INTO t_topk_nan SELECT 1, 10 + number, toFloat64(100 + number) FROM numbers(5);
INSERT INTO t_topk_nan VALUES (2, 20, nan), (2, 21, nan);

SELECT '-- Float64 with NaN';
SELECT countIf(explain LIKE '%Description: TopK%')
FROM (EXPLAIN indexes = 1 SELECT f FROM t_topk_nan ORDER BY f ASC NULLS LAST LIMIT 3);
SELECT f FROM t_topk_nan ORDER BY f ASC NULLS LAST LIMIT 3;
SELECT f FROM t_topk_nan WHERE id >= 0 ORDER BY f ASC NULLS LAST LIMIT 3;
SELECT f FROM t_topk_nan ORDER BY f DESC NULLS FIRST LIMIT 3;
SELECT f FROM t_topk_nan ORDER BY f ASC NULLS FIRST LIMIT 3;
DROP TABLE t_topk_nan;

-- Read-time skip and best-first part order, with `WHERE`.
DROP TABLE IF EXISTS t_topk_dyn;
CREATE TABLE t_topk_dyn (p UInt8, id UInt64, ts UInt64, v Nullable(Int64), tag UInt8)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 100, auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_topk_dyn
SELECT
    intDiv(number, 1000) AS p,
    number AS id,
    intDiv(number, 1000) * 10000 + (number % 1000) AS ts,
    if(intDiv(number, 1000) = 0, NULL, toInt64(intDiv(number, 1000) * 10000 + (number % 1000))) AS v,
    number % 10 AS tag
FROM numbers(10000)
SETTINGS max_partitions_per_insert_block = 20, max_insert_threads = 1, max_threads = 1;

SELECT '-- Dynamic skipping';
SELECT ts FROM t_topk_dyn WHERE tag = 3 ORDER BY ts DESC LIMIT 5 SETTINGS max_threads = 1, log_comment = 'topk_stats_dyn_on';
SELECT ts FROM t_topk_dyn WHERE tag = 3 ORDER BY ts DESC LIMIT 5 SETTINGS max_threads = 1, use_statistics_for_part_pruning = 0, log_comment = 'topk_stats_dyn_off';
SELECT ts FROM t_topk_dyn WHERE tag = 3 ORDER BY ts DESC LIMIT 5 SETTINGS max_threads = 4;
SELECT v FROM t_topk_dyn WHERE tag = 3 ORDER BY v ASC NULLS FIRST LIMIT 5 SETTINGS max_threads = 1;
SELECT v FROM t_topk_dyn WHERE tag = 3 ORDER BY v ASC NULLS LAST LIMIT 5 SETTINGS max_threads = 1;

SYSTEM FLUSH LOGS query_log;
SELECT minIf(read_rows, log_comment = 'topk_stats_dyn_on') < minIf(read_rows, log_comment = 'topk_stats_dyn_off') / 2
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment IN ('topk_stats_dyn_on', 'topk_stats_dyn_off');
DROP TABLE t_topk_dyn;

-- Gates: `FINAL`, `SAMPLE`, lightweight delete, mutations, patch parts, alters, row policy.
SELECT '-- Safety gates';
DROP TABLE IF EXISTS t_topk_final;
CREATE TABLE t_topk_final (p UInt8, id UInt64, v Int64, ver UInt64)
ENGINE = ReplacingMergeTree(ver) PARTITION BY p ORDER BY id
SETTINGS auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_topk_final VALUES (0, 1, 1, 1), (0, 2, 2, 1);
INSERT INTO t_topk_final VALUES (1, 1, 100, 2), (1, 2, 200, 2);
SELECT countIf(explain LIKE '%Description: TopK%')
FROM (EXPLAIN indexes = 1 SELECT v FROM t_topk_final FINAL ORDER BY v ASC LIMIT 1);
SELECT v FROM t_topk_final FINAL ORDER BY v ASC LIMIT 1;
DROP TABLE t_topk_final;

DROP TABLE IF EXISTS t_topk_sample;
CREATE TABLE t_topk_sample (p UInt8, id UInt32, h UInt32, v UInt32)
ENGINE = MergeTree PARTITION BY p ORDER BY (id, h) SAMPLE BY h
SETTINGS auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_topk_sample SELECT 0, number, 4294967295, number FROM numbers(100);
INSERT INTO t_topk_sample SELECT 1, 100 + number, 1073741824, 1000 + number FROM numbers(100);
SELECT v FROM t_topk_sample SAMPLE 1/2 ORDER BY v ASC LIMIT 3;
DROP TABLE t_topk_sample;

DROP TABLE IF EXISTS t_topk_lwd;
CREATE TABLE t_topk_lwd (p UInt8, id UInt64, v Int64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_topk_lwd SELECT 0, number, 1000000 + number FROM numbers(10);
INSERT INTO t_topk_lwd SELECT 1, 100 + number, number FROM numbers(10);
DELETE FROM t_topk_lwd WHERE p = 0;
SELECT v FROM t_topk_lwd ORDER BY v DESC LIMIT 3;
DROP TABLE t_topk_lwd;

DROP TABLE IF EXISTS t_topk_mut;
CREATE TABLE t_topk_mut (p UInt8, id UInt64, v Int64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_topk_mut;
INSERT INTO t_topk_mut SELECT 0, number, 1000 + number FROM numbers(10);
INSERT INTO t_topk_mut SELECT 1, 100 + number, number FROM numbers(10);
ALTER TABLE t_topk_mut UPDATE v = -1 WHERE p = 0 SETTINGS mutations_sync = 0;
SELECT v FROM t_topk_mut ORDER BY v DESC LIMIT 3 SETTINGS apply_mutations_on_fly = 1;
DROP TABLE t_topk_mut;

DROP TABLE IF EXISTS t_topk_lwu;
CREATE TABLE t_topk_lwu (p UInt8, id UInt64, v Int64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0,
         enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_topk_lwu SELECT 0, number, number FROM numbers(10);
INSERT INTO t_topk_lwu SELECT 1, 100 + number, 100 + number FROM numbers(10);
UPDATE t_topk_lwu SET v = 10000 WHERE p = 0 AND id = 0 SETTINGS allow_experimental_lightweight_update = 1, enable_lightweight_update = 1;
SELECT v FROM t_topk_lwu ORDER BY v DESC LIMIT 3 SETTINGS apply_patch_parts = 1;
DROP TABLE t_topk_lwu;

DROP TABLE IF EXISTS t_topk_alter;
CREATE TABLE t_topk_alter (p UInt8, id UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_topk_alter;
INSERT INTO t_topk_alter SELECT 0, number, number FROM numbers(10);
INSERT INTO t_topk_alter SELECT 1, 100 + number, 100 + number FROM numbers(10);
ALTER TABLE t_topk_alter DROP COLUMN v SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_topk_alter ADD COLUMN v UInt64 DEFAULT 500 - id SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT v FROM t_topk_alter ORDER BY v DESC LIMIT 3 SETTINGS apply_mutations_on_fly = 1;
DROP TABLE t_topk_alter;

DROP TABLE IF EXISTS t_topk_modify;
CREATE TABLE t_topk_modify (p UInt8, id UInt64, v UInt64, tag UInt8)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_topk_modify;
INSERT INTO t_topk_modify SELECT 0, number, 13830554455654793216, 1 FROM numbers(5);
INSERT INTO t_topk_modify SELECT 1, 100 + number, 5, 1 FROM numbers(5);
ALTER TABLE t_topk_modify MODIFY COLUMN v Float64 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT v FROM t_topk_modify ORDER BY v DESC LIMIT 1 SETTINGS apply_mutations_on_fly = 1;
SELECT v FROM t_topk_modify WHERE tag = 1 ORDER BY v DESC LIMIT 1 SETTINGS apply_mutations_on_fly = 1;
DROP TABLE t_topk_modify;

DROP ROW POLICY IF EXISTS rp_topk_stats ON t_topk_rp;
DROP TABLE IF EXISTS t_topk_rp;
CREATE TABLE t_topk_rp (p UInt8, id UInt64, visible UInt8, v Int64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_topk_rp SELECT 0, number, 0, number FROM numbers(10);
INSERT INTO t_topk_rp SELECT 1, 100 + number, 1, 100 + number FROM numbers(10);
CREATE ROW POLICY rp_topk_stats ON t_topk_rp FOR SELECT USING visible = 1 TO CURRENT_USER;
SELECT countIf(explain LIKE '%Description: TopK%')
FROM (EXPLAIN indexes = 1 SELECT v FROM t_topk_rp ORDER BY v ASC LIMIT 3);
SELECT v FROM t_topk_rp ORDER BY v ASC LIMIT 3;
DROP ROW POLICY rp_topk_stats ON t_topk_rp;
DROP TABLE t_topk_rp;
