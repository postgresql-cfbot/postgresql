--
-- Tests to exercise the plan caching/invalidation mechanism
--

CREATE TEMP TABLE pcachetest AS SELECT * FROM int8_tbl;

-- create and use a cached plan
PREPARE prepstmt AS SELECT * FROM pcachetest;

EXECUTE prepstmt;

-- and one with parameters
PREPARE prepstmt2(bigint) AS SELECT * FROM pcachetest WHERE q1 = $1;

EXECUTE prepstmt2(123);

-- invalidate the plans and see what happens
DROP TABLE pcachetest;

EXECUTE prepstmt;
EXECUTE prepstmt2(123);

-- recreate the temp table (this demonstrates that the raw plan is
-- purely textual and doesn't depend on OIDs, for instance)
CREATE TEMP TABLE pcachetest AS SELECT * FROM int8_tbl ORDER BY 2;

EXECUTE prepstmt;
EXECUTE prepstmt2(123);

-- prepared statements should prevent change in output tupdesc,
-- since clients probably aren't expecting that to change on the fly
ALTER TABLE pcachetest ADD COLUMN q3 bigint;

EXECUTE prepstmt;
EXECUTE prepstmt2(123);

-- but we're nice guys and will let you undo your mistake
ALTER TABLE pcachetest DROP COLUMN q3;

EXECUTE prepstmt;
EXECUTE prepstmt2(123);

-- Try it with a view, which isn't directly used in the resulting plan
-- but should trigger invalidation anyway
CREATE TEMP VIEW pcacheview AS
  SELECT * FROM pcachetest;

PREPARE vprep AS SELECT * FROM pcacheview;

EXECUTE vprep;

CREATE OR REPLACE TEMP VIEW pcacheview AS
  SELECT q1, q2/2 AS q2 FROM pcachetest;

EXECUTE vprep;

-- Check basic SPI plan invalidation

create function cache_test(int) returns int as $$
declare total int;
begin
	create temp table t1(f1 int);
	insert into t1 values($1);
	insert into t1 values(11);
	insert into t1 values(12);
	insert into t1 values(13);
	select sum(f1) into total from t1;
	drop table t1;
	return total;
end
$$ language plpgsql;

select cache_test(1);
select cache_test(2);
select cache_test(3);

-- Check invalidation of plpgsql "simple expression"

create temp view v1 as
  select 2+2 as f1;

create function cache_test_2() returns int as $$
begin
	return f1 from v1;
end$$ language plpgsql;

select cache_test_2();

create or replace temp view v1 as
  select 2+2+4 as f1;
select cache_test_2();

create or replace temp view v1 as
  select 2+2+4+(select max(unique1) from tenk1) as f1;
select cache_test_2();

--- Check that change of search_path is honored when re-using cached plan

create schema s1
  create table abc (f1 int);

create schema s2
  create table abc (f1 int);

insert into s1.abc values(123);
insert into s2.abc values(456);

set search_path = s1;

prepare p1 as select f1 from abc;

execute p1;

set search_path = s2;

select f1 from abc;

execute p1;

alter table s1.abc add column f2 float8;   -- force replan

execute p1;

drop schema s1 cascade;
drop schema s2 cascade;

reset search_path;

-- Check that invalidation deals with regclass constants

create temp sequence seq;

prepare p2 as select nextval('seq');

execute p2;

drop sequence seq;

create temp sequence seq;

execute p2;

-- Check DDL via SPI, immediately followed by SPI plan re-use
-- (bug in original coding)

create function cachebug() returns void as $$
declare r int;
begin
  drop table if exists temptable cascade;
  create temp table temptable as select * from generate_series(1,3) as f1;
  create temp view vv as select * from temptable;
  for r in select * from vv loop
    raise notice '%', r;
  end loop;
end$$ language plpgsql;

select cachebug();
select cachebug();

-- Check that addition or removal of any partition is correctly dealt with by
-- default partition table when it is being used in prepared statement.
create table pc_list_parted (a int) partition by list(a);
create table pc_list_part_null partition of pc_list_parted for values in (null);
create table pc_list_part_1 partition of pc_list_parted for values in (1);
create table pc_list_part_def partition of pc_list_parted default;
prepare pstmt_def_insert (int) as insert into pc_list_part_def values($1);
-- should fail
execute pstmt_def_insert(null);
execute pstmt_def_insert(1);
create table pc_list_part_2 partition of pc_list_parted for values in (2);
execute pstmt_def_insert(2);
alter table pc_list_parted detach partition pc_list_part_null;
-- should be ok
execute pstmt_def_insert(null);
drop table pc_list_part_1;
-- should be ok
execute pstmt_def_insert(1);
drop table pc_list_parted, pc_list_part_null;
deallocate pstmt_def_insert;

-- Test plan_cache_mode

create table test_mode (a int) with (autovacuum_enabled = false);
insert into test_mode select 1 from generate_series(1,1000) union all select 2;

-- ANALYZE before creating the index. CREATE INDEX scans the table, which may
-- set pages all-visible via on-access pruning. If relallvisible is then updated
-- by ANALYZE, the generic plan may pick an index-only scan instead of the
-- expected sequential scan.
analyze test_mode;
create index on test_mode (a);

prepare test_mode_pp (int) as select count(*) from test_mode where a = $1;
select name, generic_plans, custom_plans from pg_prepared_statements
  where  name = 'test_mode_pp';

-- up to 5 executions, custom plan is used
set plan_cache_mode to auto;
explain (costs off) execute test_mode_pp(2);
select name, generic_plans, custom_plans from pg_prepared_statements
  where  name = 'test_mode_pp';

-- force generic plan
set plan_cache_mode to force_generic_plan;
explain (costs off) execute test_mode_pp(2);
select name, generic_plans, custom_plans from pg_prepared_statements
  where  name = 'test_mode_pp';

-- get to generic plan by 5 executions
set plan_cache_mode to auto;
execute test_mode_pp(1); -- 1x
execute test_mode_pp(1); -- 2x
execute test_mode_pp(1); -- 3x
execute test_mode_pp(1); -- 4x
select name, generic_plans, custom_plans from pg_prepared_statements
  where  name = 'test_mode_pp';
execute test_mode_pp(1); -- 5x
select name, generic_plans, custom_plans from pg_prepared_statements
  where  name = 'test_mode_pp';

-- we should now get a really bad plan
explain (costs off) execute test_mode_pp(2);

-- but we can force a custom plan
set plan_cache_mode to force_custom_plan;
explain (costs off) execute test_mode_pp(2);
select name, generic_plans, custom_plans from pg_prepared_statements
  where  name = 'test_mode_pp';

drop table test_mode;

-- Set up a table spanning multiple heap pages for batch qualification tests.
create table batch_param_test (a int, t text, padding text)
  with (autovacuum_enabled = false);
insert into batch_param_test
  select i, 'value-' || i, repeat('x', 500)
  from generate_series(1, 200) i;

-- Test multiple batch qualifications.
select count(*) from batch_param_test
  where a > 50 and a < 151 and t <> 'value-100' \gset batch_multi_const_
\echo :batch_multi_const_count

-- Test batch qualifications followed by regular qualification.
explain (analyze, costs off, timing off, summary off, buffers off)
select * from batch_param_test where a > 50 and length(t) = 8;

-- Test projection with batch qualification in both scan directions.
begin;
declare batch_multi_scroll scroll cursor for
  select a + 1 as projected, upper(t) as projected_text
  from batch_param_test
  where a > 50 and a < 55 and length(t) = 8;
fetch forward 1 from batch_multi_scroll \gset batch_scroll_first_
fetch forward 1 from batch_multi_scroll \gset batch_scroll_second_
fetch backward 1 from batch_multi_scroll \gset batch_scroll_backward_
fetch forward 1 from batch_multi_scroll \gset batch_scroll_again_
\echo :batch_scroll_first_projected :batch_scroll_first_projected_text
\echo :batch_scroll_second_projected :batch_scroll_second_projected_text
\echo :batch_scroll_backward_projected :batch_scroll_backward_projected_text
\echo :batch_scroll_again_projected :batch_scroll_again_projected_text
close batch_multi_scroll;
commit;

-- Check the projection result slot at end of scan.
select a + 1 as projected
from batch_param_test
where a > 200 and length(t) = 8;

-- Test batch qualification with external parameters in generic plans.
set plan_cache_mode = force_generic_plan;
prepare batch_param_int_rev(int) as
  select count(*) from batch_param_test where $1 > a;
prepare batch_param_multi(int, int, text) as
  select count(*) from batch_param_test
  where a > $1 and a < $2 and t <> $3;
prepare batch_param_rest(int, text) as
  select count(*) from batch_param_test
  where a > $1 and t <> $2 and length(t) = 8;

explain (costs off) execute batch_param_rest(50, 'value-75');
execute batch_param_int_rev(101) \gset batch_int_rev_
execute batch_param_multi(50, 151, 'value-100') \gset batch_multi_first_
execute batch_param_multi(100, 201, 'value-150') \gset batch_multi_second_
execute batch_param_multi(null, 151, 'value-100') \gset batch_multi_null_first_
execute batch_param_multi(50, 151, null) \gset batch_multi_null_last_
\echo :batch_int_rev_count :batch_multi_first_count :batch_multi_second_count :batch_multi_null_first_count :batch_multi_null_last_count
execute batch_param_rest(50, 'value-75') \gset batch_rest_first_
execute batch_param_rest(90, 'value-95') \gset batch_rest_second_
\echo :batch_rest_first_count :batch_rest_second_count

-- Test PL/pgSQL parameter compilation with batch qualification.
create function batch_param_plpgsql(p int, q text)
returns table (int_result bigint, text_result bigint)
language plpgsql as $$
begin
  return query
    select (select count(*) from batch_param_test b where b.a < p),
           (select count(*) from batch_param_test b where b.t = q);
end
$$;

select * from batch_param_plpgsql(1, 'value-100') \gset batch_plpgsql_first_
select * from batch_param_plpgsql(101, 'missing') \gset batch_plpgsql_second_
select * from batch_param_plpgsql(null, null) \gset batch_plpgsql_null_
\echo :batch_plpgsql_first_int_result :batch_plpgsql_first_text_result :batch_plpgsql_second_int_result :batch_plpgsql_second_text_result :batch_plpgsql_null_int_result :batch_plpgsql_null_text_result

drop function batch_param_plpgsql(int, text);

-- Test batch qualification with internal executor parameters.
create table batch_param_exec_test (id int, p int, q text);
insert into batch_param_exec_test values
  (1, 1, 'value-2'),
  (2, 200, 'missing'),
  (3, null, null),
  (4, 199, 'value-200');

-- OFFSET 0 prevents conversion of EXISTS, keeping the correlated condition
-- as a scan qualification.
explain (costs off)
select exists (select from batch_param_test b
               where o.p < b.a and b.t <> o.q and length(b.t) = 8
               offset 0)
from batch_param_exec_test o;

select array_agg(exists (select from batch_param_test b
                         where o.p < b.a and b.t <> o.q and length(b.t) = 8
                         offset 0)
                 order by o.id) as combined_results
from batch_param_exec_test o;

drop table batch_param_exec_test;
reset plan_cache_mode;
drop table batch_param_test;
