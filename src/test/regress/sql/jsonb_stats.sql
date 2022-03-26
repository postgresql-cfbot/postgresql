CREATE OR REPLACE FUNCTION explain_jsonb(sql_query text)
RETURNS TABLE(explain_line json) AS
$$
BEGIN
	RETURN QUERY EXECUTE 'EXPLAIN (ANALYZE, FORMAT json) ' || sql_query;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_plan_and_actual_rows(sql_query text)
RETURNS TABLE(plan integer, actual integer) AS
$$
	SELECT
		(plan->>'Plan Rows')::integer plan,
		(plan->>'Actual Rows')::integer actual
	FROM (
		SELECT explain_jsonb(sql_query) #> '{0,Plan,Plans,0}'
	) p(plan)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION check_estimate(sql_query text, accuracy real)
RETURNS boolean AS
$$
	SELECT plan BETWEEN actual / (1 + accuracy) AND (actual + 1) * (1 + accuracy)
	FROM (SELECT * FROM get_plan_and_actual_rows(sql_query)) x
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION check_estimate2(sql_query text, accuracy real)
RETURNS TABLE(min integer, max integer) AS
$$
	SELECT (actual * (1 - accuracy))::integer, ((actual + 1) * (1 + accuracy))::integer
	FROM (SELECT * FROM get_plan_and_actual_rows(sql_query)) x
$$ LANGUAGE sql;

CREATE TABLE jsonb_stats_test(js jsonb);

INSERT INTO jsonb_stats_test SELECT NULL FROM generate_series(1, 1000);

INSERT INTO jsonb_stats_test SELECT 'null' FROM generate_series(1, 200);
INSERT INTO jsonb_stats_test SELECT 'true' FROM generate_series(1, 300);
INSERT INTO jsonb_stats_test SELECT 'false' FROM generate_series(1, 500);

INSERT INTO jsonb_stats_test SELECT '12345' FROM generate_series(1, 100);
INSERT INTO jsonb_stats_test SELECT (1000 * (i % 10))::text::jsonb FROM generate_series(1, 400) i;
INSERT INTO jsonb_stats_test SELECT i::text::jsonb FROM generate_series(1, 500) i;

INSERT INTO jsonb_stats_test SELECT '"foo"' FROM generate_series(1, 100);
INSERT INTO jsonb_stats_test SELECT format('"bar%s"', i % 10)::jsonb FROM generate_series(1, 400) i;
INSERT INTO jsonb_stats_test SELECT format('"baz%s"', i)::jsonb FROM generate_series(1, 500) i;

INSERT INTO jsonb_stats_test SELECT '{}' FROM generate_series(1, 100);
INSERT INTO jsonb_stats_test SELECT jsonb_build_object('foo', 'bar') FROM generate_series(1, 100);
INSERT INTO jsonb_stats_test SELECT jsonb_build_object('foo', 'baz' || (i % 10)) FROM generate_series(1, 300) i;
INSERT INTO jsonb_stats_test SELECT jsonb_build_object('foo', i % 10) FROM generate_series(1, 200) i;
INSERT INTO jsonb_stats_test SELECT jsonb_build_object('"foo \"bar"', i % 10) FROM generate_series(1, 200) i;

INSERT INTO jsonb_stats_test SELECT '[]' FROM generate_series(1, 100);
INSERT INTO jsonb_stats_test SELECT '["foo"]' FROM generate_series(1, 200);
INSERT INTO jsonb_stats_test SELECT '[12345]' FROM generate_series(1, 300);
INSERT INTO jsonb_stats_test SELECT '[["foo"]]' FROM generate_series(1, 200);
INSERT INTO jsonb_stats_test SELECT '[{"key": "foo"}]' FROM generate_series(1, 200);
INSERT INTO jsonb_stats_test SELECT '[null, "foo"]' FROM generate_series(1, 200);
INSERT INTO jsonb_stats_test SELECT '[null, 12345]' FROM generate_series(1, 300);
INSERT INTO jsonb_stats_test SELECT '[null, ["foo"]]' FROM generate_series(1, 200);
INSERT INTO jsonb_stats_test SELECT '[null, {"key": "foo"}]' FROM generate_series(1, 200);

-- Build random variable-length integer arrays
SELECT setseed(0.0);

INSERT INTO jsonb_stats_test
SELECT jsonb_build_object('array',
	jsonb_build_array())
FROM generate_series(1, 1000);

INSERT INTO jsonb_stats_test
SELECT jsonb_build_object('array',
	jsonb_build_array(
		floor(random() * 10)::int))
FROM generate_series(1, 4000);

INSERT INTO jsonb_stats_test
SELECT jsonb_build_object('array',
	jsonb_build_array(
		floor(random() * 10)::int,
		floor(random() * 10)::int))
FROM generate_series(1, 3000);

INSERT INTO jsonb_stats_test
SELECT jsonb_build_object('array',
	jsonb_build_array(
		floor(random() * 10)::int,
		floor(random() * 10)::int,
		floor(random() * 10)::int))
FROM generate_series(1, 2000);


ANALYZE jsonb_stats_test;

CREATE OR REPLACE FUNCTION check_jsonb_stats_test_estimate(sql_condition text, accuracy real)
RETURNS boolean AS
$$
	SELECT check_estimate('SELECT count(*) FROM jsonb_stats_test WHERE ' || sql_condition, accuracy)
$$ LANGUAGE sql;

DROP FUNCTION IF EXISTS check_jsonb_stats_test_estimate2(text, real);

CREATE OR REPLACE FUNCTION check_jsonb_stats_test_estimate2(sql_condition text, accuracy real)
RETURNS TABLE(plan integer, actual integer) AS
$$
	SELECT get_plan_and_actual_rows('SELECT count(*) FROM jsonb_stats_test WHERE ' || sql_condition)
$$ LANGUAGE sql;

-- Check NULL estimate
SELECT check_jsonb_stats_test_estimate($$js IS NULL$$, 0.03);
SELECT check_jsonb_stats_test_estimate($$js -> 'bad_key' IS NULL$$, 0.01);
SELECT check_jsonb_stats_test_estimate($$js #> '{bad_key}' IS NULL$$, 0.01);
SELECT check_jsonb_stats_test_estimate($$js -> 1000000 IS NULL$$, 0.01);
SELECT check_jsonb_stats_test_estimate($$js #> '{1000000}' IS NULL$$, 0.01);
SELECT check_jsonb_stats_test_estimate($$js -> 'bad_key1' -> 'bad_key2' IS NULL$$, 0.01);
SELECT check_jsonb_stats_test_estimate($$js #> '{bad_key1,bad_key2}' IS NULL$$, 0.01);
SELECT check_jsonb_stats_test_estimate($$js -> 'bad_key1' -> 1 IS NULL$$, 0.01);
SELECT check_jsonb_stats_test_estimate($$js #> '{bad_key1,1}' IS NULL$$, 0.01);
SELECT check_jsonb_stats_test_estimate($$js -> 1000000 -> 'foo' IS NULL$$, 0.01);
SELECT check_jsonb_stats_test_estimate($$js #> '{1000000,foo}' IS NULL$$, 0.01);

SELECT check_jsonb_stats_test_estimate($$js -> 'bad_key' = '123'$$, 0.01);
SELECT check_jsonb_stats_test_estimate($$js -> 1000000 = '123'$$, 0.01);

-- Check null eq estimate
SELECT check_jsonb_stats_test_estimate($$js =  'null'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js @> 'null'$$, 0.1);

-- Check boolean eq estimate
SELECT check_jsonb_stats_test_estimate($$js =  'true'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js @> 'true'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js =  'false'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js @> 'false'$$, 0.1);

-- Check numeric eq estimate
SELECT check_jsonb_stats_test_estimate($$js = '12345'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js#>'{}' = '12345'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js = '3000'$$, 0.3);
SELECT check_jsonb_stats_test_estimate($$js = '1234'$$, 1);
SELECT check_jsonb_stats_test_estimate($$js @> '6000'$$, 0.2);

-- Check numeric range estimate
SELECT check_jsonb_stats_test_estimate($$js < '0'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js < '100'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js < '1000'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js < '3456'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js < '10000'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js < '100000'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js > '100' AND js < '600'$$, 0.5);
SELECT check_jsonb_stats_test_estimate($$js > '6800' AND js < '12000'$$, 0.1);

-- Check string eq estimate
SELECT check_jsonb_stats_test_estimate($$js = '"foo"'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js = '"bar7"'$$, 0.2);
SELECT check_jsonb_stats_test_estimate($$js = '"baz1234"'$$, 10);
SELECT check_jsonb_stats_test_estimate($$js @> '"bar4"'$$, 0.3);

-- Check string range estimate
SELECT check_jsonb_stats_test_estimate($$js > '"foo"'$$, 0.01);
SELECT check_jsonb_stats_test_estimate($$js > '"bar"'$$, 0.01);
SELECT check_jsonb_stats_test_estimate($$js > '"baz"'$$, 0.01);

-- Check object eq estimate
SELECT check_jsonb_stats_test_estimate($$js = '{}'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js > '{}'$$, 0.1);

-- Check object key eq estimate
SELECT check_jsonb_stats_test_estimate($$js -> 'foo' = '"bar"'$$, 0.2);
SELECT check_jsonb_stats_test_estimate($$js -> 'foo' = '"baz"'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js -> 'foo' = '"baz5"'$$, 0.3);
SELECT check_jsonb_stats_test_estimate($$js #> '{foo}' = '"bar"'$$, 0.2);

-- Check object key range estimate
SELECT check_jsonb_stats_test_estimate($$js -> 'foo' >= '"baz2"'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js -> 'foo' <  '"baz9"'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js -> 'foo' >= '"baz2"' AND js -> 'foo' < '"baz9"'$$, 0.1);

-- Check array eq estimate
SELECT check_jsonb_stats_test_estimate($$js = '[]'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js >= '[]' AND js < '{}'$$, 0.1);

-- Check variable-length array element eq estimate
SELECT check_jsonb_stats_test_estimate($$js -> 'array' -> 0 = '1'$$, 0.2);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' -> 1 = '6'$$, 0.2);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' -> 2 = '8'$$, 0.2);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' -> 3 = '1'$$, 0.2);

-- Check variable-length array element range estimate
SELECT check_jsonb_stats_test_estimate($$js -> 'array' -> 0 < '7'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' -> 1 < '7'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' -> 2 < '7'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' -> 3 < '7'$$, 0.1);

-- Check variable-length array containment estimate
SELECT check_jsonb_stats_test_estimate($$js -> 'array' @> '[]'$$, 0.2);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' @> '[1]'$$, 0.2);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' @> '[100]'$$, 0.2);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' @> '[1, 2]'$$, 1);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' @> '[1, 100]'$$, 1);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' @> '[1, 2, 100]'$$, 1);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' @> '[1, 2, 3]'$$, 5);

SELECT check_jsonb_stats_test_estimate($$js -> 'array' @> '1'$$, 0.3);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' @> '100'$$, 10);

SELECT check_jsonb_stats_test_estimate($$js -> 'array' -> 0 @> '1'$$, 0.3);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' -> 1 @> '1'$$, 0.3);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' -> 2 @> '1'$$, 0.3);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' -> 3 @> '1'$$, 0.3);
SELECT check_jsonb_stats_test_estimate($$js -> 'array' -> 0 @> '[1]'$$, 0.3);

SELECT check_jsonb_stats_test_estimate($$js @> '{"array": []}'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js @> '{"array": [1]}'$$, 0.3);
SELECT check_jsonb_stats_test_estimate($$js @> '{"array": [100]}'$$, 0.3);
SELECT check_jsonb_stats_test_estimate($$js @> '{"array": [1, 2]}'$$, 1);
SELECT check_jsonb_stats_test_estimate($$js @> '{"array": [1, 100]}'$$, 1);
SELECT check_jsonb_stats_test_estimate($$js @> '{"array": [1, 2, 100]}'$$, 1);
SELECT check_jsonb_stats_test_estimate($$js @> '{"array": [1, 2, 3]}'$$, 3);

-- check misc containment
SELECT check_jsonb_stats_test_estimate($$js @> '"foo"'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js @> '12345'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js @> '[]'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js @> '[12345]'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js @> '["foo"]'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js @> '[["foo", "bar"]]'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js @> '[["foo"]]'$$, 0.2);
SELECT check_jsonb_stats_test_estimate($$js @> '[{"key": "foo"}]'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js @> '[null]'$$, 0.3);

-- Check object key null estimate
SELECT check_jsonb_stats_test_estimate($$js -> 'foo' IS NULL$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js -> 'foo' IS NOT NULL$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js -> '"foo \"bar"' IS NOT NULL$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js -> 'bad_key' IS NULL$$, 0.01);
SELECT check_jsonb_stats_test_estimate($$js -> 'bad_key' IS NOT NULL$$, 0.01);

-- Check object key existence
SELECT check_jsonb_stats_test_estimate($$js ? 'bad_key'$$, 10);
SELECT check_jsonb_stats_test_estimate($$js ? 'foo'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js ? 'array'$$, 0.1);

SELECT check_jsonb_stats_test_estimate($$js ?| '{foo,bad_key}'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js ?| '{foo,array}'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js ?& '{foo,bad_key}'$$, 0.1);
SELECT check_jsonb_stats_test_estimate($$js ?& '{foo,bar}'$$, 0.1);
