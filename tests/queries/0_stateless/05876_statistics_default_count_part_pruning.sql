-- `basic` `default_count` prunes non-Nullable parts: all-default, zero-default, size subcolumns, and ranges that straddle the default.

SET min_marks_per_part_to_load_statistics = 0;
SET allow_statistics = 1;
SET use_statistics_for_part_pruning = 1;
SET materialize_statistics_on_insert = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET optimize_trivial_count_with_sparsity_filter = 0;

-- String, including the `.size` subcolumn (`with_size_stream`) and `single_stream`.
DROP TABLE IF EXISTS test_default_count_str;
CREATE TABLE test_default_count_str
(
    bucket UInt8,
    payload UInt64,
    s String STATISTICS(basic)
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY tuple()
SETTINGS auto_statistics_types = '', string_serialization_version = 'with_size_stream';

-- Parts: all `''`, no `''`, no `''`, mixed.
INSERT INTO test_default_count_str VALUES (0, 1, ''), (0, 2, '');
INSERT INTO test_default_count_str VALUES (1, 10, 'a'), (1, 20, 'b');
INSERT INTO test_default_count_str VALUES (2, 100, 'err'), (2, 200, 'error');
INSERT INTO test_default_count_str VALUES (3, 1000, ''), (3, 2000, 'warn');

-- `s = ''` becomes `s.size = 0` when both settings are on.
SELECT 'String size subcolumn';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str WHERE s = '' SETTINGS optimize_functions_to_subcolumns = 1, optimize_empty_string_comparisons = 1);
SELECT sum(payload) FROM test_default_count_str WHERE s = '' SETTINGS optimize_functions_to_subcolumns = 1, optimize_empty_string_comparisons = 1;

SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str WHERE s != '' SETTINGS optimize_functions_to_subcolumns = 1, optimize_empty_string_comparisons = 1);
SELECT sum(payload) FROM test_default_count_str WHERE s != '' SETTINGS optimize_functions_to_subcolumns = 1, optimize_empty_string_comparisons = 1;

-- Comparisons and `empty` stay on the column when subcolumn rewrites are off.
SELECT 'String comparisons and empty';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str WHERE s = '' SETTINGS optimize_functions_to_subcolumns = 0, optimize_empty_string_comparisons = 0);
SELECT sum(payload) FROM test_default_count_str WHERE s = '' SETTINGS optimize_functions_to_subcolumns = 0, optimize_empty_string_comparisons = 0;

SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str WHERE s != '' SETTINGS optimize_functions_to_subcolumns = 0, optimize_empty_string_comparisons = 0);
SELECT sum(payload) FROM test_default_count_str WHERE s != '' SETTINGS optimize_functions_to_subcolumns = 0, optimize_empty_string_comparisons = 0;

SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str WHERE empty(s) SETTINGS optimize_functions_to_subcolumns = 0);
SELECT sum(payload) FROM test_default_count_str WHERE empty(s) SETTINGS optimize_functions_to_subcolumns = 0;

SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str WHERE notEmpty(s) SETTINGS optimize_functions_to_subcolumns = 0);
SELECT sum(payload) FROM test_default_count_str WHERE notEmpty(s) SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'String non-empty predicates prune the all-default part';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str WHERE s = 'err');
SELECT sum(payload) FROM test_default_count_str WHERE s = 'err';

SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str WHERE s LIKE 'er%');
SELECT sum(payload) FROM test_default_count_str WHERE s LIKE 'er%';

SELECT 'String predicates that cannot prune do not read statistics';
SELECT countIf(explain LIKE '%Statistics%') = 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str WHERE s != 'err');
SELECT countIf(explain LIKE '%Statistics%') = 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str WHERE s LIKE '%er%');
SELECT countIf(explain LIKE '%Statistics%') = 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str WHERE length(s) < 10 SETTINGS optimize_functions_to_subcolumns = 1);
SELECT countIf(explain LIKE '%Statistics%') = 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str WHERE s IN ('', 'err'));

SELECT 'String pruning disabled';
SELECT countIf(explain LIKE '%Statistics%') = 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str WHERE s = '' SETTINGS use_statistics_for_part_pruning = 0);

DROP TABLE test_default_count_str;

-- 1b. String with `single_stream` (no size subcolumn).
DROP TABLE IF EXISTS test_default_count_str_single;
CREATE TABLE test_default_count_str_single
(
    bucket UInt8,
    payload UInt64,
    s String STATISTICS(basic)
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY tuple()
SETTINGS auto_statistics_types = '', string_serialization_version = 'single_stream';

INSERT INTO test_default_count_str_single VALUES (0, 1, ''), (0, 2, '');
INSERT INTO test_default_count_str_single VALUES (1, 10, 'x'), (1, 20, 'y');

SELECT 'String single_stream';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 1/2%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str_single WHERE s = '');
SELECT sum(payload) FROM test_default_count_str_single WHERE s = '';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 1/2%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_str_single WHERE s != '');
SELECT sum(payload) FROM test_default_count_str_single WHERE s != '';

DROP TABLE test_default_count_str_single;

-- Non-numeric types. `Tuple` default is not a minimum, so `t = (0, '')` cannot prune.
DROP TABLE IF EXISTS test_default_count_types;
CREATE TABLE test_default_count_types
(
    bucket UInt8,
    payload UInt64,
    lc LowCardinality(String) STATISTICS(basic),
    fs FixedString(4) STATISTICS(basic),
    arr Array(Int32) STATISTICS(basic),
    m Map(String, UInt64) STATISTICS(basic),
    u UUID STATISTICS(basic),
    ip IPv6 STATISTICS(basic),
    t Tuple(Int32, String) STATISTICS(basic)
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY tuple()
SETTINGS auto_statistics_types = '';

-- Part 0 is all defaults. Part 1 has none. Part 2 is mixed.
INSERT INTO test_default_count_types VALUES
    (0, 1, '', '\0\0\0\0', [], map(), toUUID('00000000-0000-0000-0000-000000000000'), toIPv6('::'), (0, '')),
    (0, 2, '', '\0\0\0\0', [], map(), toUUID('00000000-0000-0000-0000-000000000000'), toIPv6('::'), (0, ''));
-- Part 1: zero defaults
INSERT INTO test_default_count_types VALUES
    (1, 10, 'foo', 'abcd', [1, 2], map('k', 1), toUUID('11111111-1111-1111-1111-111111111111'), toIPv6('::1'), (-1, 'a')),
    (1, 20, 'bar', 'efgh', [3], map('k', 2), toUUID('22222222-2222-2222-2222-222222222222'), toIPv6('::2'), (1, 'b'));
-- Part 2: mixed
INSERT INTO test_default_count_types VALUES
    (2, 100, '', '\0\0\0\0', [], map(), toUUID('00000000-0000-0000-0000-000000000000'), toIPv6('::'), (0, '')),
    (2, 200, 'baz', 'zzzz', [9], map('z', 9), toUUID('33333333-3333-3333-3333-333333333333'), toIPv6('::3'), (2, 'c'));

SELECT 'LowCardinality(String)';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE lc = '');
SELECT sum(payload) FROM test_default_count_types WHERE lc = '';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE lc != '');
SELECT sum(payload) FROM test_default_count_types WHERE lc != '';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE lc = 'foo');
SELECT sum(payload) FROM test_default_count_types WHERE lc = 'foo';
SELECT countIf(explain LIKE '%Statistics%') = 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE lc != 'foo');

SELECT 'FixedString(4)';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE empty(fs));
SELECT sum(payload) FROM test_default_count_types WHERE empty(fs);
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE notEmpty(fs));
SELECT sum(payload) FROM test_default_count_types WHERE notEmpty(fs);
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE fs = 'abcd');
SELECT sum(payload) FROM test_default_count_types WHERE fs = 'abcd';

SELECT 'Array';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE empty(arr) SETTINGS optimize_functions_to_subcolumns = 1);
SELECT sum(payload) FROM test_default_count_types WHERE empty(arr) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE notEmpty(arr) SETTINGS optimize_functions_to_subcolumns = 1);
SELECT sum(payload) FROM test_default_count_types WHERE notEmpty(arr) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE empty(arr) SETTINGS optimize_functions_to_subcolumns = 0);
SELECT sum(payload) FROM test_default_count_types WHERE empty(arr) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE arr = [1, 2]);
SELECT sum(payload) FROM test_default_count_types WHERE arr = [1, 2];

SELECT 'Map';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE empty(m) SETTINGS optimize_functions_to_subcolumns = 0);
SELECT sum(payload) FROM test_default_count_types WHERE empty(m) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE notEmpty(m) SETTINGS optimize_functions_to_subcolumns = 0);
SELECT sum(payload) FROM test_default_count_types WHERE notEmpty(m) SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'UUID and IPv6';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE empty(u));
SELECT sum(payload) FROM test_default_count_types WHERE empty(u);
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE notEmpty(u));
SELECT sum(payload) FROM test_default_count_types WHERE notEmpty(u);
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE empty(ip));
SELECT sum(payload) FROM test_default_count_types WHERE empty(ip);
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE notEmpty(ip));
SELECT sum(payload) FROM test_default_count_types WHERE notEmpty(ip);

SELECT 'Tuple';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE t = (1, 'b'));
SELECT sum(payload) FROM test_default_count_types WHERE t = (1, 'b');
SELECT countIf(explain LIKE '%Statistics%') = 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_types WHERE t = (0, ''));
SELECT sum(payload) FROM test_default_count_types WHERE t = (0, '');

DROP TABLE test_default_count_types;

-- `default_count = 0` while min/max still contain the default. `Date32` default is `1970-01-01`, not `1900-01-01`.
DROP TABLE IF EXISTS test_default_count_split;
CREATE TABLE test_default_count_split
(
    bucket UInt8,
    payload UInt64,
    i Int64 STATISTICS(basic),
    dec Decimal64(2) STATISTICS(basic),
    d32 Date32 STATISTICS(basic),
    e Enum8('a' = -1, 'b' = 0, 'c' = 1) STATISTICS(basic),
    e_nozero Enum8('x' = 1, 'y' = 2, 'z' = 3) STATISTICS(basic)
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY tuple()
SETTINGS auto_statistics_types = '';

-- Part 0 is all column defaults (`e_nozero` stores `'x'`, whose code is not 0). Part 1 straddles the default and contains none. Part 2 contains the default.
INSERT INTO test_default_count_split VALUES
    (0, 1, 0, 0.00, '1970-01-01', 'b', 'x'),
    (0, 2, 0, 0.00, '1970-01-01', 'b', 'x');
-- Part 1: straddles default with default_count == 0
INSERT INTO test_default_count_split VALUES
    (1, 10, -10, -5.50, '1900-01-01', 'a', 'x'),
    (1, 20, 10, 5.50, '2020-01-01', 'c', 'z');
-- Part 2: mixed including default
INSERT INTO test_default_count_split VALUES
    (2, 100, -10, -5.50, '1900-01-01', 'a', 'x'),
    (2, 200, 0, 0.00, '1970-01-01', 'b', 'y'),
    (2, 400, 10, 5.50, '2020-01-01', 'c', 'z');

SELECT 'Signed Int64 split-range';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_split WHERE i = 0);
SELECT sum(payload) FROM test_default_count_split WHERE i = 0;

SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_split WHERE i != 0);
SELECT sum(payload) FROM test_default_count_split WHERE i != 0;

SELECT 'Decimal64(2) split-range';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_split WHERE dec = 0.00);
SELECT sum(payload) FROM test_default_count_split WHERE dec = 0.00;

SELECT 'Date32';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_split WHERE d32 = '1970-01-01');
SELECT sum(payload) FROM test_default_count_split WHERE d32 = '1970-01-01';

SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_split WHERE d32 = '1900-01-01');
SELECT sum(payload) FROM test_default_count_split WHERE d32 = '1900-01-01';

SELECT 'Enum8';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/3%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(payload) FROM test_default_count_split WHERE e = 'b');
SELECT sum(payload) FROM test_default_count_split WHERE e = 'b';
SELECT sum(payload) FROM test_default_count_split WHERE e_nozero = 'x';
SELECT sum(payload) FROM test_default_count_split WHERE e_nozero = 'y';

DROP TABLE test_default_count_split;

-- `-0.0` is not counted in `default_count` but `f = 0` matches it. `NaN` must not be dropped either.
DROP TABLE IF EXISTS test_default_count_float;
CREATE TABLE test_default_count_float
(
    bucket UInt8,
    payload UInt64,
    f Float64 STATISTICS(basic)
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY tuple()
SETTINGS auto_statistics_types = '';

-- Part 0 contains `-0.0` (`default_count = 0`). Part 1 is all `+0.0`. Part 2 contains `-0.0` and `NaN`.
INSERT INTO test_default_count_float VALUES (0, 1, -1.0), (0, 2, -0.0), (0, 4, 1.0);
-- Part 1: all +0.0
INSERT INTO test_default_count_float VALUES (1, 10, 0.0), (1, 20, 0.0);
-- Part 2: contains NaN and -0.0
INSERT INTO test_default_count_float VALUES (2, 100, -1.0), (2, 200, -0.0), (2, 400, nan);

SELECT 'Float64 -0 and NaN';
SELECT sum(payload) FROM test_default_count_float WHERE f = 0.0;
SELECT sum(payload) FROM test_default_count_float WHERE f != 0.0;

DROP TABLE test_default_count_float;

-- `Nullable(String)` defaults are NULL, not `''`.
DROP TABLE IF EXISTS test_default_count_nullable_str;
CREATE TABLE test_default_count_nullable_str
(
    bucket UInt8,
    payload UInt64,
    ns Nullable(String) STATISTICS(basic)
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY tuple()
SETTINGS auto_statistics_types = '', nullable_serialization_version = 'basic';

INSERT INTO test_default_count_nullable_str VALUES (0, 1, NULL), (0, 2, NULL);
INSERT INTO test_default_count_nullable_str VALUES (1, 10, ''), (1, 20, '');
INSERT INTO test_default_count_nullable_str VALUES (2, 100, 'ok'), (2, 200, NULL);

SELECT 'Nullable(String)';
SELECT sum(payload) FROM test_default_count_nullable_str WHERE ns = '';
SELECT sum(payload) FROM test_default_count_nullable_str WHERE empty(ns);
SELECT sum(payload) FROM test_default_count_nullable_str WHERE notEmpty(ns);
SELECT sum(payload) FROM test_default_count_nullable_str WHERE ns IS NULL;

DROP TABLE test_default_count_nullable_str;

-- DDL `DEFAULT` is not the value `default_count` counts.
DROP TABLE IF EXISTS test_default_count_ddl_default;
CREATE TABLE test_default_count_ddl_default
(
    bucket UInt8,
    payload UInt64,
    x Int64 DEFAULT 42 STATISTICS(basic),
    s String DEFAULT 'abc' STATISTICS(basic)
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY tuple()
SETTINGS auto_statistics_types = '';

INSERT INTO test_default_count_ddl_default (bucket, payload) VALUES (0, 1), (0, 2);
INSERT INTO test_default_count_ddl_default VALUES (1, 10, 0, ''), (1, 20, 0, '');

SELECT 'DDL DEFAULT';
SELECT sum(payload) FROM test_default_count_ddl_default WHERE x = 42;
SELECT sum(payload) FROM test_default_count_ddl_default WHERE s = 'abc';
SELECT sum(payload) FROM test_default_count_ddl_default WHERE s = '';

DROP TABLE test_default_count_ddl_default;

-- A real column named `s.size` or `arr.size0` is not the virtual size subcolumn.
DROP TABLE IF EXISTS test_default_count_literal_size;
CREATE TABLE test_default_count_literal_size
(
    bucket UInt8,
    payload UInt64,
    `s.size` UInt64 STATISTICS(basic),
    `arr.size0` UInt64 STATISTICS(basic)
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY tuple()
SETTINGS auto_statistics_types = '';

INSERT INTO test_default_count_literal_size VALUES (0, 1, 0, 0), (0, 2, 0, 0);
INSERT INTO test_default_count_literal_size VALUES (1, 10, 5, 3), (1, 20, 6, 4);

SELECT 'Literal size column names';
SELECT sum(payload) FROM test_default_count_literal_size WHERE `s.size` = 0;
SELECT sum(payload) FROM test_default_count_literal_size WHERE `arr.size0` > 0;

DROP TABLE test_default_count_literal_size;
