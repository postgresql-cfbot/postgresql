--
-- Tests for pg_partition_children
--
create table ptif_test (a int, b int) partition by range (a);
create table ptif_test0 partition of ptif_test for values from (minvalue) to (0) partition by list (b);
create table ptif_test01 partition of ptif_test0 for values in (1);
create table ptif_test1 partition of ptif_test for values from (0) to (100) partition by list (b);
create table ptif_test11 partition of ptif_test1 for values in (1);
create table ptif_test2 partition of ptif_test for values from (100) to (maxvalue);
insert into ptif_test select i, 1 from generate_series(-500, 500) i;

-- all tables in the tree
select *, pg_relation_size(relid) as size from pg_partition_children('ptif_test');

-- all table excluding the root
select *, pg_relation_size(relid) as size from pg_partition_children('ptif_test') where level > 0;

-- all leaf partitions
select * from pg_partition_children('ptif_test') where isleaf;

-- total size of all partitions
select sum(pg_relation_size(relid)) as total_size from pg_partition_children('ptif_test');

-- total size of first level partitions
select sum(pg_relation_size(relid)) as total_size from pg_partition_children('ptif_test') where level = 1;

-- check that passing a lower-level table to pg_partition_children works
select *, pg_relation_size(relid) as size from pg_partition_children('ptif_test0');
select *, pg_relation_size(relid) as size from pg_partition_children('ptif_test01');
select sum(pg_relation_size(relid)) as total_size from pg_partition_children('ptif_test01');

-- this one should result in null, as there are no level 1 partitions of a leaf partition
select sum(pg_relation_size(relid)) as total_size from pg_partition_children('ptif_test01') where level = 1;

drop table ptif_test;
