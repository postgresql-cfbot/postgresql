CREATE TABLE heaptest (a integer, b text);

-- Check that invalid skip option is rejected
SELECT * FROM verify_heapam(relation := 'heaptest', skip := 'rope');

-- Check that block range is reject for an empty table
SELECT * FROM verify_heapam(relation := 'heaptest', startblock := 0, endblock := 0);

-- Check that valid options are not rejected nor corruption reported
-- for an empty table
SELECT * FROM verify_heapam(relation := 'heaptest', skip := 'none');
SELECT * FROM verify_heapam(relation := 'heaptest', skip := 'all-frozen');
SELECT * FROM verify_heapam(relation := 'heaptest', skip := 'all-visible');

-- Add some data so subsequent tests are not entirely trivial
INSERT INTO heaptest (a, b)
	(SELECT gs, repeat('x', gs)
		FROM generate_series(1,50) gs);

-- Check that an invalid block range is rejected
SELECT * FROM verify_heapam(relation := 'heaptest', startblock := 100000, endblock := 200000);

-- Check that valid options are not rejected nor corruption reported
-- for a non-empty table
SELECT * FROM verify_heapam(relation := 'heaptest', skip := 'none');
SELECT * FROM verify_heapam(relation := 'heaptest', skip := 'all-frozen');
SELECT * FROM verify_heapam(relation := 'heaptest', skip := 'all-visible');
SELECT * FROM verify_heapam(relation := 'heaptest', startblock := 0, endblock := 0);

-- Vacuum freeze to change the xids encountered in subsequent tests
VACUUM FREEZE heaptest;

-- Check that valid options are not rejected nor corruption reported
-- for a non-empty frozen table
SELECT * FROM verify_heapam(relation := 'heaptest', skip := 'none');
SELECT * FROM verify_heapam(relation := 'heaptest', skip := 'all-frozen');
SELECT * FROM verify_heapam(relation := 'heaptest', skip := 'all-visible');
SELECT * FROM verify_heapam(relation := 'heaptest', startblock := 0, endblock := 0);

-- Check that partitioned tables (the parent ones) which don't have visibility
-- maps are rejected
create table test_partitioned (a int, b text default repeat('x', 5000))
			 partition by list (a);
select * from verify_heapam('test_partitioned',
							startblock := NULL,
							endblock := NULL);

-- Check that valid options are not rejected nor corruption reported
-- for an empty partition table (the child one)
create table test_partition partition of test_partitioned for values in (1);
select * from verify_heapam('test_partition',
							startblock := NULL,
							endblock := NULL);

-- Check that valid options are not rejected nor corruption reported
-- for a non-empty partition table (the child one)
insert into test_partitioned (a) (select 1 from generate_series(1,1000) gs);
select * from verify_heapam('test_partition',
							startblock := NULL,
							endblock := NULL);

-- Check that indexes are rejected
create index test_index on test_partition (a);
select * from verify_heapam('test_index',
							startblock := NULL,
							endblock := NULL);

-- Check that views are rejected
create view test_view as select 1;
select * from verify_heapam('test_view',
							startblock := NULL,
							endblock := NULL);

-- Check that sequences are rejected
create sequence test_sequence;
select * from verify_heapam('test_sequence',
							startblock := NULL,
							endblock := NULL);

-- Check that foreign tables are rejected
create foreign data wrapper dummy;
create server dummy_server foreign data wrapper dummy;
create foreign table test_foreign_table () server dummy_server;
select * from verify_heapam('test_foreign_table',
							startblock := NULL,
							endblock := NULL);
