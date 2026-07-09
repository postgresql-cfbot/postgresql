--
-- UNIQUEKEYS
--
-- Tests for the planner's inference of what a relation is distinct over
--

create table uk_t1 (a int, b int, c int, d int, primary key (a, b));
create table uk_t2 (id int primary key, v int);
create table uk_t3 (id int primary key, v int);
create table uk_t4 (id int primary key, v int);
create table uk_tn (nu int unique, nz int, k int);
create unique index uk_tn_nz on uk_tn (nz) nulls not distinct;
create table uk_def (id int, primary key (id) deferrable);
create table uk_part (a int, b int);
create unique index uk_part_a on uk_part (a) where b > 0;

--
-- Base relation keys
--
-- a non-nullable multi-column key removes the DISTINCT
explain (costs off)
select distinct a, b from uk_t1;
-- a key column equated to a constant is dropped from the key
explain (costs off)
select distinct b from uk_t1 where a = 5;
-- a nullable unique column is not enough on its own
explain (costs off)
select distinct nu from uk_tn;
-- a NULLS NOT DISTINCT unique column is a full key
explain (costs off)
select distinct nz from uk_tn;
-- a strict restriction makes the nullable unique column non-null
explain (costs off)
select distinct nu from uk_tn where nu > 0;
-- a deferrable unique index is unusable
explain (costs off)
select distinct id from uk_def;
-- a partial unique index is unusable
explain (costs off)
select distinct a from uk_part where b > 0;

--
-- Subquery-in-FROM keys
--
-- a DISTINCT inside is translated
explain (costs off)
select distinct a, b from (select distinct a, b from uk_t1) s;
-- a GROUP BY inside is translated
explain (costs off)
select distinct a, b from (select a, b from uk_t1 group by a, b) s;
-- a plain aggregate emits a single row
explain (costs off)
select distinct m from (select max(a) as m from uk_t1) s;
-- a key from a base index carried up through a join
explain (costs off)
select distinct x from (select uk_t2.id as x from uk_t2 join uk_t3 on uk_t2.id = uk_t3.id offset 0) s;
-- a non-ALL set operation is distinct over its whole output row (the set
-- operation's own dedup Aggregate ends up below the join)
explain (costs off)
select distinct s.a, s.b from (select a, b from uk_t1 union select a, b from uk_t1) s join uk_t2 on s.a = uk_t2.id;
-- UNION ALL is not distinct, so the DISTINCT Aggregate stays above the join
explain (costs off)
select distinct s.a, s.b from (select a, b from uk_t1 union all select a, b from uk_t1) s join uk_t2 on s.a = uk_t2.id;

--
-- Join relation keys
--
-- an inner join preserves a side's key when the other side is unique for the
-- join clauses
explain (costs off)
select distinct uk_t3.id from uk_t2 join uk_t3 on uk_t2.id = uk_t3.id;
-- combination: the union of a key from each side (here a cross join)
explain (costs off)
select distinct uk_t2.id, uk_t3.id from uk_t2 cross join uk_t3;
-- a left join makes the RHS key nullable, so the DISTINCT is kept
explain (costs off)
select distinct uk_t3.id from uk_t2 left join uk_t3 on uk_t2.id = uk_t3.id;
-- a left join preserves the non-nullable LHS key
explain (costs off)
select distinct uk_t2.id, uk_t3.v from uk_t2 left join uk_t3 on uk_t2.id = uk_t3.id;
-- a semijoin preserves the LHS key
explain (costs off)
select distinct a, b from uk_t1 where exists (select 1 from uk_t2 where uk_t2.id = uk_t1.c);
-- an inner join's strict clause strengthens a nullable key to non-nullable
explain (costs off)
select distinct nu from uk_tn join uk_t2 on uk_tn.nu = uk_t2.id;
-- the strict clause does not cover the key column, so no strengthening
explain (costs off)
select distinct nu from uk_tn join uk_t2 on uk_tn.k = uk_t2.id;
-- a full join yields no key
explain (costs off)
select distinct uk_t2.id from uk_t2 full join uk_t3 on uk_t2.id = uk_t3.id;
-- a key survives stacked outer joins
explain (costs off)
select distinct uk_t2.id, y.v from uk_t2 left join uk_t3 x on uk_t2.id = x.id left join uk_t3 y on x.id = y.id;
-- uniqueness of a multi-relation inner side is proven from that joinrel's key
explain (verbose, costs off)
select distinct uk_t2.id from uk_t2 left join (uk_t3 join uk_t4 on uk_t3.id = uk_t4.id) on uk_t2.v = uk_t3.id;
-- uniqueness of a semijoin's multi-relation RHS skips the unique-ification step
explain (costs off)
select * from uk_t2, uk_t3 where (uk_t2.id, uk_t3.id) in (select t1.a, t1.b from uk_t1 t1 join uk_tn on t1.a = nu and b = nz);

--
-- Grouping step: grouping by a superset of a key is a no-op (with no aggregates)
--
explain (costs off)
select a, b from uk_t1 group by a, b, c;
-- grouping by a non-key set is not a no-op
explain (costs off)
select a, c from uk_t1 group by a, c;

drop table uk_t1;
drop table uk_t2;
drop table uk_t3;
drop table uk_t4;
drop table uk_tn;
drop table uk_def;
drop table uk_part;
