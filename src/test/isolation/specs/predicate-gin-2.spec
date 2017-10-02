# Test for page level predicate locking in gin
#
# Test to check reduced false positives.
#
# Queries are written in such a way that an index scan(from one transaction) and an index insert(from another transaction) will try to access different parts(sub-tree) of the index.


setup
{
 create table gin_tbl(id int4, p int4[]);
 create index ginidx on gin_tbl using gin(p) with 
 (fastupdate = off);
 insert into gin_tbl select g, array[1, g*2, 2] from generate_series(11, 1000) g;
 insert into gin_tbl select g, array[3, g*2, 4] from generate_series(11, 1000) g;
}

teardown
{
 DROP TABLE gin_tbl;
}

session "s1"
setup		{ 
		  BEGIN ISOLATION LEVEL SERIALIZABLE;
		  set enable_seqscan=off;
		}
step "rxy1"	{ select sum(p[1]+p[2]+p[3]) from gin_tbl where p @> array[1,2]; }
step "wx1"	{ insert into gin_tbl select g, array[g, g*2, g*3]
		  from generate_series(3000, 3100) g;}
step "c1"	{ COMMIT; }

session "s2"
setup		{ 
		  BEGIN ISOLATION LEVEL SERIALIZABLE;
		  set enable_seqscan=off;
		}

step "rxy2"	{ select sum(p[1]+p[2]+p[3]) from gin_tbl where p @> array[3,4]; }
step "wy2"	{ insert into gin_tbl select g, array[g, g*2, g*3]
		  from generate_series(4000, 4100) g; }
step "c2"	{ COMMIT; }
