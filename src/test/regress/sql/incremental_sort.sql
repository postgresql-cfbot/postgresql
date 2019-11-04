-- When we have to sort the entire table, incremental sort will
-- be slower than plain sort, so it should not be used.
explain (costs off)
select * from (select * from tenk1 order by four) t order by four, ten;

-- When there is a LIMIT clause, incremental sort is beneficial because
-- it only has to sort some of the groups, and not the entire table.
explain (costs off)
select * from (select * from tenk1 order by four) t order by four, ten
limit 1;

-- When work_mem is not enough to sort the entire table, incremental sort
-- may be faster if individual groups still fit into work_mem.
set work_mem to '2MB';
explain (costs off)
select * from (select * from tenk1 order by four) t order by four, ten;
reset work_mem;

-- TODO if an analyze happens here the plans might change; should we
-- solve by inserting extra rows or by adding a GUC that would somehow
-- forcing the time of plan we expect.
create table t(a integer, b integer);

-- A single large group tested around each mode transition point.
insert into t(a, b) select 1, i from generate_series(1, 100) n(i);
explain (costs off) select * from (select * from t order by a) s order by a, b limit 31;
select * from (select * from t order by a) s order by a, b limit 31;
explain (costs off) select * from (select * from t order by a) s order by a, b limit 32;
select * from (select * from t order by a) s order by a, b limit 32;
explain (costs off) select * from (select * from t order by a) s order by a, b limit 33;
select * from (select * from t order by a) s order by a, b limit 33;
explain (costs off) select * from (select * from t order by a) s order by a, b limit 65;
select * from (select * from t order by a) s order by a, b limit 65;
explain (costs off) select * from (select * from t order by a) s order by a, b limit 66;
select * from (select * from t order by a) s order by a, b limit 66;
delete from t;

-- An initial large group followed by a small group.
insert into t(a, b) select (case when i < 50 then 1 else 2 end), i from generate_series(1, 100) n(i);
explain (costs off) select * from (select * from t order by a) s order by a, b limit 55;
select * from (select * from t order by a) s order by a, b limit 55;
delete from t;

-- An initial small group followed by a large group.
insert into t(a, b) select (case when i < 5 then i else 9 end), i from generate_series(1, 100) n(i);
explain (costs off) select * from (select * from t order by a) s order by a, b limit 70;
select * from (select * from t order by a) s order by a, b limit 70;
delete from t;

-- Small groups of 10 tuples each tested around each mode transition point.
insert into t(a, b) select i / 10, i from generate_series(1, 70) n(i);
explain (costs off) select * from (select * from t order by a) s order by a, b limit 31;
select * from (select * from t order by a) s order by a, b limit 31;
explain (costs off) select * from (select * from t order by a) s order by a, b limit 32;
select * from (select * from t order by a) s order by a, b limit 32;
explain (costs off) select * from (select * from t order by a) s order by a, b limit 33;
select * from (select * from t order by a) s order by a, b limit 33;
explain (costs off) select * from (select * from t order by a) s order by a, b limit 65;
select * from (select * from t order by a) s order by a, b limit 65;
explain (costs off) select * from (select * from t order by a) s order by a, b limit 66;
select * from (select * from t order by a) s order by a, b limit 66;
delete from t;

-- Small groups of only 1 tuple each tested around each mode transition point.
insert into t(a, b) select i, i from generate_series(1, 70) n(i);
explain (costs off) select * from (select * from t order by a) s order by a, b limit 31;
select * from (select * from t order by a) s order by a, b limit 31;
explain (costs off) select * from (select * from t order by a) s order by a, b limit 32;
select * from (select * from t order by a) s order by a, b limit 32;
explain (costs off) select * from (select * from t order by a) s order by a, b limit 33;
select * from (select * from t order by a) s order by a, b limit 33;
explain (costs off) select * from (select * from t order by a) s order by a, b limit 65;
select * from (select * from t order by a) s order by a, b limit 65;
explain (costs off) select * from (select * from t order by a) s order by a, b limit 66;
select * from (select * from t order by a) s order by a, b limit 66;
delete from t;

drop table t;
