-- Creating an index on a partitioned table makes the partitions
-- automatically get the index
create table idxpart (a int, b int, c text) partition by range (a);
create table idxpart1 partition of idxpart for values from (0) to (10);
create table idxpart2 partition of idxpart for values from (10) to (100)
	partition by range (b);
create table idxpart21 partition of idxpart2 for values from (0) to (100);
create index on idxpart (a);
select relname, relkind, indparentidx::regclass
    from pg_class left join pg_index on (indexrelid = oid)
	where relname like 'idxpart%' order by relname;
drop table idxpart;

-- Some unsupported cases
create table idxpart (a int, b int, c text) partition by range (a);
create table idxpart1 partition of idxpart for values from (0) to (10);
create index concurrently on idxpart (a);
drop table idxpart;

-- If a table without index is attached as partition to a table with
-- an index, the index is automatically created
create table idxpart (a int, b int, c text) partition by range (a);
create index idxparti on idxpart (a);
create index idxparti2 on idxpart (b, c);
create table idxpart1 (like idxpart);
\d idxpart1
alter table idxpart attach partition idxpart1 for values from (0) to (10);
\d idxpart1
drop table idxpart;

-- If a partition already has an index, make sure we don't create a separate
-- one
create table idxpart (a int, b int) partition by range (a, b);
create table idxpart1 partition of idxpart for values from (0, 0) to (10, 10);
create index on idxpart1 (a, b);
create index on idxpart (a, b);
\d idxpart1
select indexrelid::regclass, indrelid::regclass, indparentidx::regclass
  from pg_index where indexrelid::regclass::text like 'idxpart%'
  order by indexrelid::regclass;
create index idxpart1_tst1 on idxpart1 (b, a);
create index idxpart1_tst2 on idxpart1 using hash (a);
create index idxpart1_tst3 on idxpart1 (a, b) where a > 10;

-- ALTER INDEX .. ATTACH/DETACH, error cases
alter index idxpart attach partition idxpart1;
alter index idxpart detach partition idxpart1;
alter index idxpart_a_b_idx attach partition idxpart1;
alter index idxpart_a_b_idx detach partition idxpart1;
alter index idxpart_a_b_idx attach partition idxpart_a_b_idx;
alter index idxpart_a_b_idx attach partition idxpart1_b_idx;
alter index idxpart_a_b_idx attach partition idxpart1_tst1;
alter index idxpart_a_b_idx attach partition idxpart1_tst2;
alter index idxpart_a_b_idx attach partition idxpart1_tst3;
-- OK
alter index idxpart_a_b_idx attach partition idxpart1_a_b_idx;
alter index idxpart_a_b_idx attach partition idxpart1_a_b_idx; -- quiet
alter index idxpart_a_b_idx detach partition idxpart1_a_b_idx;
alter index idxpart_a_b_idx detach partition idxpart1_a_b_idx; -- quiet
drop table idxpart;
-- make sure everything's gone
select indexrelid::regclass, indrelid::regclass, indparentidx::regclass
  from pg_index where indexrelid::regclass::text like 'idxpart%';

-- If CREATE INDEX ONLY, don't create indexes on partitions; and existing
-- indexes on partitions don't change parent.  ALTER INDEX ATTACH can change
-- the parent after the fact.
create table idxpart (a int) partition by range (a);
create table idxpart1 partition of idxpart for values from (0) to (100);
create table idxpart2 partition of idxpart for values from (100) to (1000)
  partition by range (a);
create table idxpart21 partition of idxpart2 for values from (100) to (200);
create table idxpart22 partition of idxpart2 for values from (200) to (300);
create index on idxpart22 (a);
create index on only idxpart2 (a);
create index on idxpart (a);
-- Here we expect that idxpart1 and idxpart2 have a new index, but idxpart21
-- does not; also, idxpart22 is detached.
\d idxpart1
\d idxpart2
\d idxpart21
select indexrelid::regclass, indrelid::regclass, indparentidx::regclass
  from pg_index where indexrelid::regclass::text like 'idxpart%'
  order by indrelid::regclass;
alter index idxpart2_a_idx attach partition idxpart22_a_idx;
select indexrelid::regclass, indrelid::regclass, indparentidx::regclass
  from pg_index where indexrelid::regclass::text like 'idxpart%'
  order by indrelid::regclass;
drop table idxpart;

-- When a table is attached a partition and it already has an index, a
-- duplicate index should not get created, but rather the index becomes
-- attached to the parent's index.
create table idxpart (a int, b int, c text) partition by range (a);
create index idxparti on idxpart (a);
create index idxparti2 on idxpart (b, c);
create table idxpart1 (like idxpart including indexes);
\d idxpart1
select relname, relkind, indparentidx::regclass
    from pg_class left join pg_index on (indexrelid = oid)
	where relname like 'idxpart%' order by relname;
alter table idxpart attach partition idxpart1 for values from (0) to (10);
\d idxpart1
select relname, relkind, indparentidx::regclass
    from pg_class left join pg_index on (indexrelid = oid)
	where relname like 'idxpart%' order by relname;
drop table idxpart;

-- Make sure the partition columns are mapped correctly
create table idxpart (a int, b int, c text) partition by range (a);
create index idxparti on idxpart (a);
create index idxparti2 on idxpart (c, b);
create table idxpart1 (c text, a int, b int);
alter table idxpart attach partition idxpart1 for values from (0) to (10);
\d idxpart1
drop table idxpart;

-- Make sure things work if either table has dropped columns
create table idxpart (a int, b int, c int, d int) partition by range (a);
alter table idxpart drop column c;
create index idxparti on idxpart (a);
create index idxparti2 on idxpart (b, d);
create table idxpart1 (like idxpart);
alter table idxpart attach partition idxpart1 for values from (0) to (10);
\d idxpart1
select attrelid::regclass, attname, attnum from pg_attribute
  where attrelid in ('idxpart'::regclass, 'idxpart1'::regclass) and attnum > 0
  order by attrelid::regclass, attnum;
drop table idxpart;

create table idxpart (a int, b int, c int) partition by range (a);
create table idxpart1 (zz int, like idxpart, aa int);
alter table idxpart1 drop column zz, drop column aa;
create index idxparti on idxpart (a);
create index idxparti2 on idxpart (b, c);
alter table idxpart attach partition idxpart1 for values from (0) to (10);
\d idxpart1
select attrelid::regclass, attname, attnum from pg_attribute
  where attrelid in ('idxpart'::regclass, 'idxpart1'::regclass) and attnum > 0
  order by attrelid::regclass, attnum;
drop table idxpart;

--
-- Constraint-related indexes
--

-- Verify that it works to add primary key / unique to partitioned tables
create table idxpart (a int primary key, b int) partition by range (b);
create table idxpart (a int primary key, b int) partition by range (a);
\d idxpart
drop table idxpart;
create table idxpart (a int unique, b int) partition by range (a);
\d idxpart
drop table idxpart;
-- but not other types of index-based constraints
create table idxpart (a int, exclude (a with = )) partition by range (a);

-- It works to add primary keys after the partitioned table is created
create table idxpart (a int, b int) partition by range (a);
alter table idxpart add primary key (a);
\d idxpart
drop table idxpart;

-- It works to add unique constraints after the partitioned table is created
create table idxpart (a int, b int) partition by range (a);
alter table idxpart add unique (a);
\d idxpart
drop table idxpart;

-- Exclusion constraints cannot be added
create table idxpart (a int, b int) partition by range (a);
alter table idxpart add exclude (a with =);
drop table idxpart;

-- It is an error to add a constraint to columns that are not in the partition
-- key.
create table idxpart (a int, b int, primary key (a, b)) partition by range (a);
create table idxpart (a int, b int, primary key (a, b)) partition by range (b);
create table idxpart (a int, b int, unique (a, b)) partition by range (a);
create table idxpart (a int, b int, unique (a, b)) partition by range (b);
-- but using a partial set of columns is okay
create table idxpart (a int, b int, c int primary key) partition by range (b);
drop table idxpart;
create table idxpart (a int, b int, primary key (a)) partition by range (a, b);
drop table idxpart;
create table idxpart (a int, b int, primary key (b)) partition by range (a, b);
drop table idxpart;
create table idxpart (a int, b int, c int unique) partition by range (b);
drop table idxpart;
create table idxpart (a int, b int, unique (a)) partition by range (a, b);
drop table idxpart;
create table idxpart (a int, b int, unique (b)) partition by range (a, b);
drop table idxpart;

-- When (sub)partitions are created, they also contain the constraint
create table idxpart (a int, b int, primary key (a, b)) partition by range (a, b);
create table idxpart1 partition of idxpart for values from (1, 1) to (10, 10);
create table idxpart2 partition of idxpart for values from (10, 10) to (20, 20)
  partition by range (b);
create table idxpart21 partition of idxpart2 for values from (10) to (15);
create table idxpart22 partition of idxpart2 for values from (15) to (20);
create table idxpart3 (b int not null, a int not null);
alter table idxpart attach partition idxpart3 for values from (20, 20) to (30, 30);
select conname, contype, conrelid::regclass, conindid::regclass, conkey
  from pg_constraint where conrelid::regclass::text like 'idxpart%'
  order by conname;
drop table idxpart;

create table idxpart (a int primary key, b int) partition by range (a);
create table idxpart1 partition of idxpart for values from (1) to (10);
\d idxpart1
drop table idxpart;
