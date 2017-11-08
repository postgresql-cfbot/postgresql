# Test for page level predicate locking in gin
#
# Test to verify serialization failures
#
# Queries are written in such a way that an index scan(from one transaction) and an index insert(from another transaction) will try to access the same part(sub-tree) of the index.


setup
{
 create table gin_tbl(id int4, p int4[]);
 create index ginidx on gin_tbl using gin(p) with 
 (fastupdate = off);
 insert into gin_tbl select g, array[1, 2, g*2] from generate_series(1, 200) g;
 insert into gin_tbl select g, array[3, 4, g*3] from generate_series(1, 200) g;
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
step "rxy1"	{ select sum(p[2]) from gin_tbl where p @> array[1,2]; }
step "wx1"	{ insert into gin_tbl select g, array[3, 4, g*3] 
		  from generate_series(200, 250) g;}
step "c1"	{ COMMIT; }

session "s2"
setup		{ 
		  BEGIN ISOLATION LEVEL SERIALIZABLE;
		  set enable_seqscan=off;
		}

step "rxy2"	{ select sum(p[2]) from gin_tbl where p @> array[3,4]; }
step "wy2"	{ insert into gin_tbl select g, array[1, 2, g*2] 
		  from generate_series(200, 250) g; }
step "c2"	{ COMMIT; }
