--
-- Test partitioning planner code
--
create table lp (a char) partition by list (a);
create table lp_ef partition of lp for values in ('e', 'f');
create table lp_ad partition of lp for values in ('a', 'd');
create table lp_bc partition of lp for values in ('b', 'c');
create table lp_null partition of lp for values in (null);
explain (costs off) select * from lp;
explain (costs off) select * from lp where a > 'a' and a < 'd';
explain (costs off) select * from lp where a > 'a' and a <= 'd';
explain (costs off) select * from lp where a = 'a';
explain (costs off) select * from lp where 'a' = a;	/* commutates */
explain (costs off) select * from lp where a is not null;
explain (costs off) select * from lp where a is null;

create table rlp (a int, b varchar) partition by range (a);
create table rlp1 partition of rlp for values from (minvalue) to (1);
create table rlp2 partition of rlp for values from (1) to (10);

create table rlp3 (b varchar, a int) partition by list (b varchar_ops);
create table rlp3abcd partition of rlp3 for values in ('ab', 'cd');
create table rlp3efgh partition of rlp3 for values in ('ef', 'gh');
create table rlp3nullxy partition of rlp3 for values in (null, 'xy');
alter table rlp attach partition rlp3 for values from (15) to (20);

create table rlp4 partition of rlp for values from (20) to (30);
create table rlp5 partition of rlp for values from (31) to (maxvalue);

explain (costs off) select * from rlp where a < 1;
explain (costs off) select * from rlp where 1 > a;	/* commutates */
explain (costs off) select * from rlp where a <= 1;
explain (costs off) select * from rlp where a = 1;
explain (costs off) select * from rlp where a = 1::bigint;		/* same as above */
explain (costs off) select * from rlp where a = 1::numeric;	/* no pruning */
explain (costs off) select * from rlp where a <= 10;
explain (costs off) select * from rlp where a > 10;
explain (costs off) select * from rlp where a < 15;
explain (costs off) select * from rlp where a <= 15;
explain (costs off) select * from rlp where a > 15 and b = 'ab';
explain (costs off) select * from rlp where a = 16 and b is null;
explain (costs off) select * from rlp where a = 16 and b is not null;
explain (costs off) select * from rlp where a is null;		/* while we're on nulls */
explain (costs off) select * from rlp where a > 30;
explain (costs off) select * from rlp where a = 30;	/* empty */
explain (costs off) select * from rlp where a <= 31;

-- multi-column keys
create table mc3p (a int, b int, c int) partition by range (a, abs(b), c);
create table mc3p0 partition of mc3p for values from (minvalue, minvalue, minvalue) to (1, 1, 1);
create table mc3p1 partition of mc3p for values from (1, 1, 1) to (10, 5, 10);
create table mc3p2 partition of mc3p for values from (10, 5, 10) to (10, 10, 10);
create table mc3p3 partition of mc3p for values from (10, 10, 10) to (10, 10, 20);
create table mc3p4 partition of mc3p for values from (10, 10, 20) to (10, maxvalue, maxvalue);
create table mc3p5 partition of mc3p for values from (11, 1, 1) to (20, 10, 10);
create table mc3p6 partition of mc3p for values from (20, 10, 10) to (20, 20, 20);
create table mc3p7 partition of mc3p for values from (20, 20, 20) to (maxvalue, maxvalue, maxvalue);

explain (costs off) select * from mc3p where a = 1;
explain (costs off) select * from mc3p where a = 1 and abs(b) < 1;
explain (costs off) select * from mc3p where a = 1 and abs(b) = 1;
explain (costs off) select * from mc3p where a = 1 and abs(b) = 1 and c < 8;
explain (costs off) select * from mc3p where a = 10 and abs(b) between 5 and 35;
explain (costs off) select * from mc3p where a > 10;
explain (costs off) select * from mc3p where a >= 10;
explain (costs off) select * from mc3p where a < 10;
explain (costs off) select * from mc3p where a <= 10 and abs(b) < 10;
explain (costs off) select * from mc3p where a = 11 and abs(b) = 0;	/* empty */
explain (costs off) select * from mc3p where a = 20 and abs(b) = 10 and c = 100;
explain (costs off) select * from mc3p where a > 20;
explain (costs off) select * from mc3p where a >= 20;

-- XXX - '<>' clauses cannot be handled yet
explain (costs off) select * from lp where a <> 'a';

-- XXX - redundant clause elimination does not happen yet
explain (costs off) select * from mc3p where a = 10 and a > 1;

-- XXX - the OR clauses don't contribute to partition-pruning yet
explain (costs off) select * from rlp3 where b = 'ab' or b = 'ef';

drop table lp, rlp, mc3p;
