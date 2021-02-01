--
-- PARALLEL
--

-- Serializable isolation would disable parallel query, so explicitly use an
-- arbitrary other level.
begin isolation level repeatable read;

-- encourage use of parallel plans
set parallel_setup_cost=0;
set parallel_tuple_cost=0;
set min_parallel_table_scan_size=0;
set max_parallel_workers_per_gather=4;

--
-- Test write operations that has an underlying query that is eligble
-- for parallel plans
--
explain (costs off) create table parallel_write as
    select length(stringu1) from tenk1 group by length(stringu1);
create table parallel_write as
    select length(stringu1) from tenk1 group by length(stringu1);
drop table parallel_write;

explain (costs off) select length(stringu1) into parallel_write
    from tenk1 group by length(stringu1);
select length(stringu1) into parallel_write
    from tenk1 group by length(stringu1);
drop table parallel_write;

explain (costs off) create materialized view parallel_mat_view as
    select length(stringu1) from tenk1 group by length(stringu1);
create materialized view parallel_mat_view as
    select length(stringu1) from tenk1 group by length(stringu1);
drop materialized view parallel_mat_view;

prepare prep_stmt as select length(stringu1) from tenk1 group by length(stringu1);
explain (costs off) create table parallel_write as execute prep_stmt;
create table parallel_write as execute prep_stmt;
drop table parallel_write;

--
-- Test parallel inserts in create table as/select into/create materialized
-- view.
--

-- Parallel queries won't necessarily get as many workers as the planner
-- asked for.  This affects not only the "Workers Launched:" field of EXPLAIN
-- results, but also row counts and loop counts for parallel scans, Gathers,
-- and everything in between.  This function filters out the values we can't
-- rely on to be stable.
-- This removes enough info that you might wonder why bother with EXPLAIN
-- ANALYZE at all.  The answer is that we need to see whether the parallel
-- inserts are being done by the workers, the only way is that
-- Create <<tbl_name>> appears in the explain output.
create function explain_pictas(text) returns setof text
language plpgsql as
$$
declare
    ln text;
begin
    for ln in
        execute format('explain (analyze, costs off, summary off, timing off) %s',
            $1)
    loop
        ln := regexp_replace(ln, 'Workers Launched: \d+', 'Workers Launched: N');
        ln := regexp_replace(ln, 'actual rows=\d+ loops=\d+', 'actual rows=N loops=N');
        ln := regexp_replace(ln, 'Rows Removed by Filter: \d+', 'Rows Removed by Filter: N');
        ln := regexp_replace(ln, '\m\d+kB', 'NkB', 'g');
        ln := regexp_replace(ln, 'Buckets: \d+', 'Buckets: N');
        ln := regexp_replace(ln, 'Batches: \d+', 'Batches: N');
        return next ln;
    end loop;
end;
$$;

-- parallel inserts must occur as the CTAS creates a normal table
select explain_pictas(
'create table parallel_write as select length(stringu1) from tenk1;');
select count(*) from parallel_write;
-- check if the parallel insertions have happened within the same xact, if yes,
-- there should be a single cmin and xmin i.e. below query should output 1
select count(*) from (select distinct cmin, xmin from parallel_write) as dt;
drop table parallel_write;

-- parallel inserts must not occur as the table is temporary
select explain_pictas(
'create temporary table parallel_write as select length(stringu1) from tenk1;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel inserts must occur as the CTAS creates an unlogged table
select explain_pictas(
'create unlogged table parallel_write as select length(stringu1) from tenk1;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel inserts must occur as the select into creates a normal table
select explain_pictas(
'select length(stringu1) into parallel_write from tenk1;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel inserts must not occur as the table is temporary
select explain_pictas(
'select length(stringu1) into temporary parallel_write from tenk1;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel inserts must occur as the select into creates an unlogged table
select explain_pictas(
'select length(stringu1) into unlogged parallel_write from tenk1;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel inserts must not occur as the parallelism will not be picked
-- for select part because of for update clause
select explain_pictas(
'create table parallel_write as select length(stringu1) from tenk1 for update;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel inserts must occur as the materialized view is being created here
select explain_pictas(
'create materialized view parallel_mat_view as
    select length(stringu1) from tenk1;');
select count(*) from parallel_mat_view;
drop materialized view parallel_mat_view;

-- parallel inserts must occur as the CTAS creates the table using prepared
-- statement for which parallelism would have been picked
prepare parallel_write_prep as select length(stringu1) from tenk1;
select explain_pictas(
'create table parallel_write as execute parallel_write_prep;');
select count(*) from parallel_write;
deallocate parallel_write_prep;
drop table parallel_write;

-- parallel inserts must not occur as the parallelism will not be picked
-- for select part because of the parallel unsafe function
create sequence parallel_write_sequence;
select explain_pictas(
E'create table parallel_write as
    select nextval(\'parallel_write_sequence\'), four from tenk1;');
select count(*) from parallel_write;
drop table parallel_write;
drop sequence parallel_write_sequence;

-- parallel inserts must occur, as there is init plan that gets executed by
-- each parallel worker
select explain_pictas(
'create table parallel_write as select two col1,
    (select two from (select * from tenk2) as tt limit 1) col2
    from tenk1  where tenk1.four = 3;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel inserts must not occur, as there is sub plan that gets executed by
-- the Gather node in leader
select explain_pictas(
'create table parallel_write as select two col1,
    (select tenk1.two from generate_series(1,1)) col2
    from tenk1  where tenk1.four = 3;');
select count(*) from parallel_write;
drop table parallel_write;

create table temp1(col1) as select * from generate_series(1,5);
create table temp2(col2) as select * from temp1;
create table temp3(col3) as select * from temp1;

-- parallel inserts must not occur, as there is a limit clause
select explain_pictas(
'create table parallel_write as select * from temp1 limit 4;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel inserts must not occur, as there is an order by clause
select explain_pictas(
'create table parallel_write as select * from temp1 order by 1;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel inserts must not occur, as there is an order by clause
select explain_pictas(
'create table parallel_write as select * from temp1 order by 1;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel inserts must not occur, as there is a distinct clause
select explain_pictas(
'create table parallel_write as select distinct * from temp1;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel inserts must not occur, as there is an aggregate and group clause
select explain_pictas(
'create table parallel_write as select count(*) from temp1 group by col1;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel inserts must not occur, as there is an aggregate, group and having
-- clauses
select explain_pictas(
'create table parallel_write as
    select count(col1), (select col3 from
        (select * from temp3) as tt limit 1) col4 from temp1, temp2
    where temp1.col1 = temp2.col2 group by col4 having count(col1) > 0;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel inserts must not occur, as there is a window function
select explain_pictas(
'create table parallel_write as
    select avg(col1) OVER (PARTITION BY col1) from temp1;');
select count(*) from parallel_write;
drop table parallel_write;

-- nested loop join is the top node under which Gather node exists, so parallel
-- inserts must not occur
set enable_nestloop to on;
set enable_mergejoin to off;
set enable_hashjoin to off;

select explain_pictas(
'create table parallel_write as
    select * from temp1, temp2  where temp1.col1 = temp2.col2;');
select count(*) from parallel_write;
drop table parallel_write;

-- parallel hash join happens under Gather node, so parallel inserts must occur
set enable_hashjoin to on;
set enable_nestloop to off;

select explain_pictas(
'create table parallel_write as
    select * from temp1, temp2  where temp1.col1 = temp2.col2;');
select count(*) from parallel_write;
drop table parallel_write;

reset enable_nestloop;
reset enable_mergejoin;
reset enable_hashjoin;

-- test cases for performing parallel inserts when Append node is at the top
-- and Gather node is in one of its direct sub plans.

-- case 1: parallel inserts must occur at each Gather node as we can push the
-- CTAS dest receiver.
-- Append
-- 	->Gather
-- 		->Parallel Seq Scan
-- 	->Gather
-- 		->Parallel Seq Scan
-- 	->Gather
-- 		->Parallel Seq Scan

select explain_pictas(
'create table parallel_write as
    select * from temp1 where col1 = 5 union all
    select * from temp2 where col2 = 5 union all
    select * from temp2 where col2 = 5;');
select count(*) from parallel_write;
drop table parallel_write;

-- case 2: parallel inserts must occur at the top Gather node as we can push
-- the CTAS dest receiver to it.
-- Gather
-- 	->Parallel Append
-- 		->Parallel Seq Scan
-- 		->Parallel Seq Scan
-- 		->Parallel Seq Scan

select explain_pictas(
'create table parallel_write as
    select * from temp1 union all
    select * from temp2 union all
    select * from temp2;');
select count(*) from parallel_write;
drop table parallel_write;

select explain_pictas(
'create table parallel_write as
    select (select col2 from temp2 limit 1) col2 from temp1 union all
    select (select col2 from temp2 limit 1) col2 from temp1 union all
    select * from temp2;');
select count(*) from parallel_write;
drop table parallel_write;

-- case 3: parallel inserts must occur at each Gather node as we can push the
-- CTAS dest receiver. Non-Gather nodes will do inserts by sending tuples to
-- Append and from there to CTAS dest receiver.
-- Append
-- 	->Gather
-- 		->Parallel Seq Scan
-- 	->Seq Scan / Join / any other non-Gather node
-- 	->Gather
-- 		->Parallel Seq Scan

select explain_pictas(
'create table parallel_write as
    select * from temp1 where col1 = 5 union all
    select (select temp1.col1 from temp2 limit 1) col2 from temp1 union all
    select * from temp1 where col1 = 5;');
select count(*) from parallel_write;
drop table parallel_write;

alter table temp2 set (parallel_workers = 0);
select explain_pictas(
'create table parallel_write as select * from temp1 where col1 = (select 1) union all
    select * from temp1 where col1 = (select 1) union all
 select * from temp2 where col2 = (select 2);');
select count(*) from parallel_write;
alter table temp2 reset (parallel_workers);
drop table parallel_write;

-- case 4: parallel inserts must not occur as there will be no direct Gather
-- node under Append node. Non-Gather nodes will do inserts by sending tuples
-- to Append and from there to CTAS dest receiver.
-- Append
-- 	->Seq Scan / Join / any other non-Gather node
-- 	->Seq Scan / Join / any other non-Gather node
-- 	->Seq Scan / Join / any other non-Gather node

select explain_pictas(
'create table parallel_write as
    select * from temp1 union all
    select * from temp2 union all
    select (select temp1.col1 from temp2 limit 1) col2 from temp1;');
select count(*) from parallel_write;
drop table parallel_write;

-- case 5: parallel inserts must occur at the top Gather node as we can push
-- the CTAS dest receiver to it.
-- Gather
-- 	->Parallel Append
-- 		->Seq Scan / Join / any other non-Gather node
-- 		->Parallel Seq Scan
-- 		->Parallel Seq Scan

alter table temp2 set (parallel_workers = 0);

select explain_pictas(
'create table parallel_write as
    select * from temp1 union all
    select * from temp2 union all
    select * from temp1;');
select count(*) from parallel_write;
drop table parallel_write;

alter table temp2 reset (parallel_workers);

-- case 6: parallel inserts must occur at each Gather node as we can push the
-- CTAS dest receiver.
-- Append
--  ->Append
--     ->Gather
-- 		->Parallel Seq Scan
--  ->Append
--     ->Gather
-- 		->Parallel Seq Scan
--  ->Gather
--  ->Gather

select explain_pictas(
'create table parallel_write as
    select * from (select * from temp1 where col1 = (select 1) union all
            select * from temp2 where col2 = (select 2)) as tt
        where col1 = (select 1) union all
    select * from temp2 where col2 = (select 2) union all
    select * from temp2 where col2 = (select 2);');
select count(*) from parallel_write;
drop table parallel_write;

select explain_pictas(
'create table parallel_write as
    select * from (select * from temp1 where col1 = (select 1) union all
            select * from temp2 where col2 = (select 2)) as tt
        where col1 = (select 1) union all
    select * from (select * from temp1 where col1 = (select 1) union all
            select * from temp2 where col2 = (select 2)) as tt
        where col1 = (select 1) union all
    select * from temp1 where col1 = 5 union all
    select * from temp2 where col2 = 5;');
select count(*) from parallel_write;
drop table parallel_write;

-- case 7: parallel inserts must occur at each Gather node as we can push the
-- CTAS dest receiver. Non-Gather nodes will do inserts by sending tuples
-- to Append and from there to CTAS dest receiver.
-- Append
--  ->Append
--     ->Gather
-- 		->Parallel Seq Scan
--  ->Append
--     ->Gather
-- 		->Parallel Seq Scan
--  ->Seq Scan / Join / any other non-Gather node
--  ->Gather

alter table temp2 set (parallel_workers = 0);

select explain_pictas(
'create table parallel_write as
    select * from (select * from temp1 where col1 = (select 1) union all
            select * from temp2 where col2 = (select 2)) as tt
        where col1 = (select 1) union all
    select * from temp2 where col2 = (select 2) union all
    select * from temp1 where col1 = (select 2);');
select count(*) from parallel_write;
drop table parallel_write;

select explain_pictas(
'create table parallel_write as
    select * from (select * from temp1 where col1 = (select 1) union all
            select * from temp2 where col2 = (select 2)) as tt
        where col1 = (select 1) union all
    select * from (select * from temp1 where col1 = (select 1) union all
            select * from temp2 where col2 = (select 2)) as tt
        where col1 = (select 1) union all
    select * from temp1 where col1 = 5 union all
    select * from temp2 where col2 = 5;');
select count(*) from parallel_write;
drop table parallel_write;

alter table temp2 reset (parallel_workers);

-- case 8: parallel inserts must not occur because there is no Gather or Append
-- node at the top for union, except/except all, intersect/intersect all
-- cases.

select explain_pictas(
'create table parallel_write as
    select * from temp1 union
    select * from temp2;');
select count(*) from parallel_write;
drop table parallel_write;

select explain_pictas(
'create table parallel_write as
    select * from temp1 except
    select * from temp2 where col2 < 3;');
select count(*) from parallel_write;
drop table parallel_write;

select explain_pictas(
'create table parallel_write as
    select * from temp1 except all
    select * from temp2 where col2 < 3;');
select count(*) from parallel_write;
drop table parallel_write;

select explain_pictas(
'create table parallel_write as
    select * from temp1 intersect
    select * from temp2;');
select count(*) from parallel_write;
drop table parallel_write;

select explain_pictas(
'create table parallel_write as
    select * from temp1 intersect all
    select * from temp2;');
select count(*) from parallel_write;
drop table parallel_write;

drop table temp1;
drop table temp2;
drop table temp3;
drop function explain_pictas(text);
rollback;
