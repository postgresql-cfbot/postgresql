# Test race conditions involving:
# - s1: UPSERT a tuple
# - s2: UPSERT the same tuple
# - s3: CREATE UNIQUE INDEX CONCURRENTLY
# - s4: operations with injection points

setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA test;
	CREATE UNLOGGED TABLE test.tbl(i int primary key, updated_at timestamp);
	ALTER TABLE test.tbl SET (parallel_workers=0);
}

teardown
{
	DROP SCHEMA test CASCADE;
	DROP EXTENSION injection_points;
}

session s1
setup	{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('check_exclusion_or_unique_constraint_no_conflict', 'wait');
	SELECT injection_points_attach('invalidate_catalog_snapshot_end', 'wait');
}
step s1_start_upsert	{ INSERT INTO test.tbl VALUES(13,now()) on conflict(i) do update set updated_at = now(); }

session s2
setup	{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('exec_insert_before_insert_speculative', 'wait');
}
step s2_start_upsert	{ INSERT INTO test.tbl VALUES(13,now()) on conflict(i) do update set updated_at = now(); }

session s3
setup	{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('define_index_before_set_valid', 'wait');
}
step s3_start_create_index		{ CREATE UNIQUE INDEX CONCURRENTLY tbl_pkey_duplicate ON test.tbl(i); }

session s4
step s4_wakeup_s1		{
	SELECT injection_points_wakeup('check_exclusion_or_unique_constraint_no_conflict');
	SELECT injection_points_detach('check_exclusion_or_unique_constraint_no_conflict');
}
step s4_wakeup_s1_from_invalidate_catalog_snapshot	{
	SELECT injection_points_wakeup('invalidate_catalog_snapshot_end');
	SELECT injection_points_detach('invalidate_catalog_snapshot_end');
}
step s4_wakeup_s2		{
	SELECT injection_points_wakeup('exec_insert_before_insert_speculative');
	SELECT injection_points_detach('exec_insert_before_insert_speculative');
}
step s4_wakeup_define_index_before_set_valid	{
	SELECT injection_points_wakeup('define_index_before_set_valid');
	SELECT injection_points_detach('define_index_before_set_valid');
}

permutation
	s3_start_create_index
	s1_start_upsert
	s4_wakeup_define_index_before_set_valid
	s2_start_upsert
	s4_wakeup_s1_from_invalidate_catalog_snapshot
	s4_wakeup_s2
	s4_wakeup_s1