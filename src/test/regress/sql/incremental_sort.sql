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

