-- Tags: no-fasttest

-- Tests StatisticsBasic: unified statistics tracking min/max, null_count, and string lengths.
-- Covers:
--   * 'minmax' DDL alias is stored and displayed as 'basic' in SHOW CREATE TABLE
--   * 'basic' works on String and FixedString (new vs old minmax which only accepted numeric types)
--   * 'basic' works on Nullable columns
--   * Part-level min/max pruning produces correct query results

DROP TABLE IF EXISTS tab;

SET allow_statistics = 1;
SET mutations_sync = 1;

-- 'minmax' is a backward-compatible alias: stored and displayed as 'basic'
CREATE TABLE tab (col UInt32 STATISTICS(minmax)) ENGINE = MergeTree() ORDER BY tuple();
SHOW CREATE TABLE tab;
DROP TABLE tab;

-- 'basic' works on String and FixedString (previously unsupported by minmax)
CREATE TABLE tab (col String STATISTICS(basic)) ENGINE = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col FixedString(8) STATISTICS(basic)) ENGINE = MergeTree() ORDER BY tuple(); DROP TABLE tab;

-- 'basic' works on Nullable columns (tracks null_count alongside min/max)
CREATE TABLE tab (col Nullable(Float64) STATISTICS(basic)) ENGINE = MergeTree() ORDER BY tuple(); DROP TABLE tab;

-- Part pruning: two parts with disjoint value ranges.
-- Part 1 holds values [1..100], part 2 holds values [1001..1100].
-- A predicate n BETWEEN 1 AND 200 should prune part 2 entirely.
CREATE TABLE tab (n UInt64 STATISTICS(basic)) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO tab SELECT number + 1    FROM system.numbers LIMIT 100;
INSERT INTO tab SELECT number + 1001 FROM system.numbers LIMIT 100;

SELECT count(*) FROM tab WHERE n BETWEEN 1 AND 200;   -- part 2 pruned: expects 100
SELECT count(*) FROM tab WHERE n BETWEEN 1 AND 1200;  -- both parts: expects 200

DROP TABLE tab;
