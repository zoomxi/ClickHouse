-- `<col>.null` inside `OR` / nested filters, and `Variant`/`Dynamic` NULL-count part pruning.

SET min_marks_per_part_to_load_statistics = 0;
SET allow_statistics = 1;
SET use_statistics_for_part_pruning = 1;
SET materialize_statistics_on_insert = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_null_or_pruning;
CREATE TABLE t_null_or_pruning
(
    bucket UInt8,
    a Nullable(Int64) STATISTICS(basic),
    b Nullable(Int64) STATISTICS(basic),
    s1 Nullable(String) STATISTICS(basic),
    s2 LowCardinality(Nullable(String)) STATISTICS(basic),
    f Nullable(Float64) STATISTICS(basic),
    u String
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY tuple()
SETTINGS auto_statistics_types = '', nullable_serialization_version = 'basic';

-- Part 0: all NULL
INSERT INTO t_null_or_pruning VALUES (0, NULL, NULL, NULL, NULL, NULL, 'p0'), (0, NULL, NULL, NULL, NULL, NULL, 'p0');
-- Part 1: no NULL, small values
INSERT INTO t_null_or_pruning VALUES (1, 10, 10, 'a', 'a', 1.0, 'p1'), (1, 11, 11, 'a', 'a', 1.5, 'p1');
-- Part 2: no NULL, large `a`, `f` includes NaN
INSERT INTO t_null_or_pruning VALUES (2, 200, 10, 'b', 'b', nan, 'p2'), (2, 201, 11, 'b', 'b', 2.0, 'p2');
-- Part 3: `a`, `s1`, `f` all NULL; `b`, `s2` no NULL
INSERT INTO t_null_or_pruning VALUES (3, NULL, 10, NULL, 'c', NULL, 'p3'), (3, NULL, 11, NULL, 'c', NULL, 'p3');
-- Part 4: `a`, `s1`, `f` no NULL; `b`, `s2` all NULL
INSERT INTO t_null_or_pruning VALUES (4, 10, NULL, 'd', NULL, 2.5, 'p4'), (4, 11, NULL, 'd', NULL, 3.0, 'p4');
-- Part 5: mixed NULL and non-NULL
INSERT INTO t_null_or_pruning VALUES (5, NULL, NULL, NULL, NULL, NULL, 'p5'), (5, 50, 50, 'e', 'e', nan, 'p5');

SELECT '--- OR of two IS NULL (numeric), opt=1 and opt=0';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 4/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE a IS NULL OR b IS NULL SETTINGS optimize_functions_to_subcolumns = 1);
SELECT count() FROM t_null_or_pruning WHERE a IS NULL OR b IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 4/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE a IS NULL OR b IS NULL SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count() FROM t_null_or_pruning WHERE a IS NULL OR b IS NULL SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '--- OR of two IS NULL (String + LC(Nullable(String))), opt=1 and opt=0';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 4/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE s1 IS NULL OR s2 IS NULL SETTINGS optimize_functions_to_subcolumns = 1);
SELECT count() FROM t_null_or_pruning WHERE s1 IS NULL OR s2 IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 4/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE s1 IS NULL OR s2 IS NULL SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count() FROM t_null_or_pruning WHERE s1 IS NULL OR s2 IS NULL SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '--- Same column range OR IS NULL (both `a` and `a.null` in DAG, key deduplicated)';
SELECT
    countIf(explain LIKE '%Statistics%') > 0,
    countIf(explain LIKE '%Parts: 4/6%') > 0,
    countIf(trim(replaceAll(explain, '│', '')) = 'a') = 1
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE a > 100 OR a IS NULL SETTINGS optimize_functions_to_subcolumns = 1);
SELECT count() FROM t_null_or_pruning WHERE a > 100 OR a IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_or_pruning WHERE a > 100 OR a IS NULL SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '--- Nested (IS NULL AND range) OR (IS NULL AND range)';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 1/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE (a IS NULL AND b = 10) OR (a IS NULL AND b = 11) SETTINGS optimize_functions_to_subcolumns = 1);
SELECT count() FROM t_null_or_pruning WHERE (a IS NULL AND b = 10) OR (a IS NULL AND b = 11) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_or_pruning WHERE (a IS NULL AND b = 10) OR (a IS NULL AND b = 11) SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '--- NOT (a IS NULL AND b IS NULL)';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 5/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE NOT (a IS NULL AND b IS NULL) SETTINGS optimize_functions_to_subcolumns = 1);
SELECT count() FROM t_null_or_pruning WHERE NOT (a IS NULL AND b IS NULL) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_or_pruning WHERE NOT (a IS NULL AND b IS NULL) SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '--- Range AND (s1 IS NULL OR b IS NOT NULL) prunes Part 4 too';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE a > 5 AND (s1 IS NULL OR b IS NOT NULL) SETTINGS optimize_functions_to_subcolumns = 1);
SELECT count() FROM t_null_or_pruning WHERE a > 5 AND (s1 IS NULL OR b IS NOT NULL) SETTINGS optimize_functions_to_subcolumns = 1;

SELECT '--- Unprunable OR drops dead `.null` key and does not load statistics';
SELECT countIf(explain LIKE '%Statistics%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE a IS NULL OR u = 'p1' SETTINGS optimize_functions_to_subcolumns = 1);
SELECT count() FROM t_null_or_pruning WHERE a IS NULL OR u = 'p1' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT
    countIf(explain LIKE '%Statistics%') > 0,
    countIf(explain LIKE '%Parts: 2/6%') > 0,
    countIf(trim(replaceAll(explain, '│', '')) = 'a') = 1,
    countIf(trim(replaceAll(explain, '│', '')) = 's1') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE a > 40 AND (s1 IS NULL OR u = 'p2') SETTINGS optimize_functions_to_subcolumns = 1);
SELECT count() FROM t_null_or_pruning WHERE a > 40 AND (s1 IS NULL OR u = 'p2') SETTINGS optimize_functions_to_subcolumns = 1;

SELECT '--- Float64 with NaN: IS NOT NULL / NOT (IS NULL) still prunes all-NULL parts and keeps NaN';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 4/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE f IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 1);
SELECT count() FROM t_null_or_pruning WHERE f IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 4/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE NOT (f IS NULL) SETTINGS optimize_functions_to_subcolumns = 1);
SELECT count() FROM t_null_or_pruning WHERE NOT (f IS NULL) SETTINGS optimize_functions_to_subcolumns = 1;

SELECT '--- Explicit `.null` comparisons respect `[1, 255]` semantics';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE a.null = 1);
SELECT count() FROM t_null_or_pruning WHERE a.null = 1;
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 4/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_or_pruning WHERE a.null = 0);
SELECT count() FROM t_null_or_pruning WHERE a.null = 0;
SELECT count() FROM t_null_or_pruning WHERE a.null != 1;

DROP TABLE t_null_or_pruning;

SELECT '--- Nullable(JSON) `.null` path inside OR is not mistaken for null-map';
DROP TABLE IF EXISTS t_json_or_pruning;
CREATE TABLE t_json_or_pruning
(
    bucket UInt8,
    a Nullable(Int64) STATISTICS(basic),
    json Nullable(JSON) STATISTICS(basic)
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY tuple()
SETTINGS auto_statistics_types = '', nullable_serialization_version = 'basic';

INSERT INTO t_json_or_pruning VALUES (0, 1, '{"null": 1}'), (0, 2, '{"null": 2}');
INSERT INTO t_json_or_pruning VALUES (1, 200, '{"other": 3}');

SELECT countIf(explain LIKE '%Statistics%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_json_or_pruning WHERE json.null = 1 OR a > 100);
SELECT count() FROM t_json_or_pruning WHERE json.null = 1 OR a > 100;

DROP TABLE t_json_or_pruning;

SELECT '--- Variant and Dynamic NULL-count part pruning';
DROP TABLE IF EXISTS t_variant_dynamic_pruning;
CREATE TABLE t_variant_dynamic_pruning
(
    bucket UInt8,
    x Int64 STATISTICS(basic),
    v Variant(Int64, String) STATISTICS(basic),
    vf Variant(Int64, Float64) STATISTICS(basic),
    d Dynamic STATISTICS(basic),
    dn Dynamic STATISTICS(basic),
    u String
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY tuple()
SETTINGS auto_statistics_types = '', nullable_serialization_version = 'basic';

-- Part 0: all NULL
INSERT INTO t_variant_dynamic_pruning VALUES (0, 1, NULL, NULL, NULL, NULL, 'p0'), (0, 2, NULL, NULL, NULL, NULL, 'p0');
-- Part 1: no NULL (Int64)
INSERT INTO t_variant_dynamic_pruning VALUES (1, 10, 10::Int64, 10::Int64, 10::Int64, 10::Int64, 'p1'), (1, 11, 11::Int64, 11::Int64, 11::Int64, 11::Int64, 'p1');
-- Part 2: no NULL (String / Float64, large x)
INSERT INTO t_variant_dynamic_pruning VALUES (2, 200, 'abc', 1.5::Float64, 'abc', 1.5::Float64, 'p2'), (2, 201, 'def', 2.5::Float64, 'def', 2.5::Float64, 'p2');
-- Part 3: mixed NULL and non-NULL
INSERT INTO t_variant_dynamic_pruning VALUES (3, 50, NULL, NULL, NULL, NULL, 'p3'), (3, 60, 20::Int64, 20::Int64, 'xyz', 20::Int64, 'p3');
-- Part 4: v/vf all NULL, d/dn no NULL
INSERT INTO t_variant_dynamic_pruning VALUES (4, 5, NULL, NULL, 30::Int64, 30::Int64, 'p4'), (4, 6, NULL, NULL, 31::Int64, 31::Int64, 'p4');
-- Part 5: v/vf no NULL, d/dn all NULL
INSERT INTO t_variant_dynamic_pruning VALUES (5, 5, 40::Int64, 40::Int64, NULL, NULL, 'p5'), (5, 6, 41::Int64, 41::Int64, NULL, NULL, 'p5');

SELECT 'v IS NULL';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_variant_dynamic_pruning WHERE v IS NULL);
SELECT count() FROM t_variant_dynamic_pruning WHERE v IS NULL;

SELECT 'v IS NOT NULL';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 4/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_variant_dynamic_pruning WHERE v IS NOT NULL);
SELECT count() FROM t_variant_dynamic_pruning WHERE v IS NOT NULL;

SELECT 'd IS NULL';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_variant_dynamic_pruning WHERE d IS NULL);
SELECT count() FROM t_variant_dynamic_pruning WHERE d IS NULL;

SELECT 'd IS NOT NULL';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 4/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_variant_dynamic_pruning WHERE d IS NOT NULL);
SELECT count() FROM t_variant_dynamic_pruning WHERE d IS NOT NULL;

SELECT 'v IS NULL OR d IS NULL';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 4/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_variant_dynamic_pruning WHERE v IS NULL OR d IS NULL);
SELECT count() FROM t_variant_dynamic_pruning WHERE v IS NULL OR d IS NULL;

SELECT 'v IS NULL OR x > 100';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 4/6%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_variant_dynamic_pruning WHERE v IS NULL OR x > 100);
SELECT count() FROM t_variant_dynamic_pruning WHERE v IS NULL OR x > 100;

SELECT 'unprunable v IS NULL OR u = p1';
SELECT countIf(explain LIKE '%Statistics%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_variant_dynamic_pruning WHERE v IS NULL OR u = 'p1');
SELECT count() FROM t_variant_dynamic_pruning WHERE v IS NULL OR u = 'p1';

SELECT 'value comparisons on Variant/Dynamic stay exact (incl. NOT / != / NOT IN)';
SELECT count() FROM t_variant_dynamic_pruning WHERE vf = 10;
SELECT count() FROM t_variant_dynamic_pruning WHERE vf != 10;
SELECT count() FROM t_variant_dynamic_pruning WHERE NOT (vf != 10);
SELECT count() FROM t_variant_dynamic_pruning WHERE vf IN (10::Int64, 20::Int64);
SELECT count() FROM t_variant_dynamic_pruning WHERE vf NOT IN (10::Int64, 20::Int64);
SELECT count() FROM t_variant_dynamic_pruning WHERE NOT (vf NOT IN (10::Int64, 20::Int64));
SELECT count() FROM t_variant_dynamic_pruning WHERE vf IS NULL OR vf != 10;
SELECT count() FROM t_variant_dynamic_pruning WHERE vf IS NULL OR vf NOT IN (10::Int64, 20::Int64);
SELECT count() FROM t_variant_dynamic_pruning WHERE dn = 10;
SELECT count() FROM t_variant_dynamic_pruning WHERE dn != 10;
SELECT count() FROM t_variant_dynamic_pruning WHERE NOT (dn != 10);
SELECT count() FROM t_variant_dynamic_pruning WHERE dn != nan;
SELECT count() FROM t_variant_dynamic_pruning WHERE dn IS NULL OR dn != 10;

DROP TABLE t_variant_dynamic_pruning;
