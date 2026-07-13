CREATE TABLE uk_t (id int primary key, a int not null, b int not null, c int, d int, e int);

explain (costs off)
select distinct id from uk_t;

explain (costs off)
select distinct e from uk_t where id = e;

create unique index on uk_t (a, b);
create unique index on uk_t (c, d);

explain (costs off)
select distinct a, b from uk_t;

explain (costs off)
select distinct c, d from uk_t;

explain (costs off)
select distinct c, d from uk_t
where c > 0 and d > 0;

explain (costs off)
select distinct d from uk_t
where c > 1 and d > 0;

explain (costs off)
select distinct d from uk_t
where c = 1 and d > 0;


-- test the join case --

EXPLAIN (COSTS OFF)
SELECT distinct t1.id FROM uk_t t1 JOIN uk_t t2 ON t1.e = t2.id;

EXPLAIN (COSTS OFF)
SELECT distinct t1.id FROM uk_t t1 JOIN uk_t t2 ON t1.e = t2.a and t1.d = t2.b;


EXPLAIN (COSTS OFF)
SELECT distinct t1.id FROM uk_t t1 LEFT JOIN uk_t t2 ON t1.e = t2.id;

EXPLAIN (COSTS OFF)
SELECT distinct t1.id FROM uk_t t1 LEFT JOIN uk_t t2 ON t1.e = t2.a and t1.d = t2.b;

-- outer join makes null values so distinct can't be a no-op.
EXPLAIN (COSTS OFF)
SELECT distinct t1.id FROM uk_t t1 RIGHT JOIN uk_t t2 ON t1.e = t2.id;

-- single row case.

-- single row is unqiue all the time.

-- case 1:
-- t1.d is single row at base rel level, and because of t1.e = t2.id, it is
-- still single row after join.
EXPLAIN (COSTS OFF)
SELECT distinct t1.d FROM uk_t t1 JOIN uk_t t2 ON t1.e = t2.id and t1.id = 1;

-- case 2
-- any uniquekey which join with single row whose uniqueness can be kept.
-- t1.d is single row at base rel level because of t1.id = 1
-- so t2's uniquekey can be kept when join with t2.any_column.

EXPLAIN (COSTS OFF)
SELECT distinct t2.id FROM uk_t t1 JOIN uk_t t2 ON t2.e = t1.e and t1.id = 1;

insert into uk_t SELECT 1, 2, 3, 4, 5, 6;

-- combined uniquekey cases.
-- the combinations of uniquekeys from the 2 sides is still unique
-- no matter the join method and join clauses.
EXPLAIN (COSTS OFF)
SELECT distinct t2.id, t1.id FROM uk_t t1 JOIN uk_t t2 ON true;
SELECT distinct t2.id, t1.id FROM uk_t t1 JOIN uk_t t2 ON true;

EXPLAIN (COSTS OFF)
SELECT distinct t2.id, t1.id FROM uk_t t1 JOIN uk_t t2 ON false;
SELECT distinct t2.id, t1.id FROM uk_t t1 JOIN uk_t t2 ON false;


-- BAD CASE: the distinct should be marked as noop in the below 4 cases, but
-- since it is nullable by outer join, so it can't be removed. However the rule
-- here should be if both uniquekey are not nullable, the combinations of them
-- is not *mutli* nullable even by outer join. It is not clear to me how to
-- implement it since I am just feeling not maintaining the not-null stuff
-- during the joins is pretty amazing, otherwise there are too many stuff to
-- do to cover this special case.
EXPLAIN (COSTS OFF)
SELECT distinct t2.id, t1.id FROM uk_t t1 LEFT JOIN uk_t t2 ON true;
SELECT distinct t2.id, t1.id FROM uk_t t1 LEFT JOIN uk_t t2 ON true;

EXPLAIN (COSTS OFF)
SELECT distinct t2.id, t1.id FROM uk_t t1 LEFT JOIN uk_t t2 ON false;
SELECT distinct t2.id, t1.id FROM uk_t t1 LEFT JOIN uk_t t2 ON false;


EXPLAIN (COSTS OFF)
SELECT distinct t2.id, t1.id FROM uk_t t1 FULL JOIN uk_t t2 ON true;
SELECT distinct t2.id, t1.id FROM uk_t t1 FULL JOIN uk_t t2 ON true;

EXPLAIN (COSTS OFF)
SELECT distinct t2.id, t1.id FROM uk_t t1 FULL JOIN uk_t t2 ON false;
SELECT distinct t2.id, t1.id FROM uk_t t1 FULL JOIN uk_t t2 ON false;
