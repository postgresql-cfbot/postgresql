# Test for page level predicate locking in gist
#
# Test to check false positives.
#
# Queries are written in such a way that an index scan(from one transaction) and an index insert(from another transaction) will try to access the different part(sub-tree) of the index.


setup
{
 create table gist_point_tbl(id int4, p point);
 create index gist_pointidx on gist_point_tbl using gist(p);
 insert into gist_point_tbl (id, p)
 select g, point(g*10, g*10) from generate_series(1, 1000) g;
}

teardown
{
 DROP TABLE gist_point_tbl;
}

session "s1"
setup		{ 
		  BEGIN ISOLATION LEVEL SERIALIZABLE;
		  set enable_seqscan=off;
		  set enable_bitmapscan=off;
		  set enable_indexonlyscan=on;
		}
step "rxy1"	{ select sum(p[0]) from gist_point_tbl where p >> point(6000,6000); }
step "wx1"	{ insert into gist_point_tbl (id, p)
                  select g, point(g*500, g*500) from generate_series(12, 18) g; }
step "c1"	{ COMMIT; }

session "s2"
setup		{ 
		  BEGIN ISOLATION LEVEL SERIALIZABLE;
		  set enable_seqscan=off;
		  set enable_bitmapscan=off;
		  set enable_indexonlyscan=on;
		}

step "rxy2"	{ select sum(p[0]) from gist_point_tbl where p << point(1000,1000); }
step "wy2"	{ insert into gist_point_tbl (id, p)
		  select g, point(g*50, g*50) from generate_series(1, 20) g; }
step "c2"	{ COMMIT; }
