# Test for page level predicate locking in spgist
#
# Test to verify serialization failures.
#
# Queries are written in such a way that an index scan(from one transaction) and an index insert(from another transaction) will try to access the same part(sub-tree) of the index.

setup
{
 create table spgist_box_tbl(id int, p box);
 create index spgist_boxidx on spgist_box_tbl using spgist(p);
 insert into spgist_box_tbl (id, p)
 select g, box(point(10000-g*10, 10000-g*10),point(10000+g*10, 10000+g*10))
 from generate_series(1, 1000) g;
}

teardown
{
 DROP TABLE spgist_box_tbl;
}

session "s1"
setup		{
		  BEGIN ISOLATION LEVEL SERIALIZABLE;
		  set enable_seqscan=off;
		  set enable_bitmapscan=off;
		  set enable_indexonlyscan=on;
		}
step "rxy1"	{ select sum(id) from spgist_box_tbl where
		  p @> box(point(5000,5000),point(15000,15000)); }
step "wx1"	{ insert into spgist_box_tbl (id, p)
                  select g, box(point(10000-g*500,10000-g*500),point(10000+g*500,10000+g*500))
		  from generate_series(1, 10) g; }
step "c1"	{ COMMIT; }

session "s2"
setup		{
		  BEGIN ISOLATION LEVEL SERIALIZABLE;
		  set enable_seqscan=off;
		  set enable_bitmapscan=off;
		  set enable_indexonlyscan=on;
		}

step "rxy2"	{ select sum(id) from spgist_box_tbl where
		  p <@ box(point(5000,5000),point(15000,15000)); }
step "wy2"	{ insert into spgist_box_tbl (id, p)
		  select g, box(point(10000-g*500,10000-g*500),point(10000+g*500,10000+g*500))
		  from generate_series(11, 20) g; }
step "c2"	{ COMMIT; }
