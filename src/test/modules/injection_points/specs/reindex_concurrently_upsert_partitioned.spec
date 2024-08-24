# Test race conditions involving:
# - s1: UPSERT a tuple
# - s2: UPSERT the same tuple
# - s3: REINDEX concurrent primary key index
# - s4: operations with injection points

setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA test;
	CREATE TABLE test.tbl(i int primary key, updated_at timestamp) PARTITION BY RANGE (i);
	CREATE TABLE test.tbl_partition PARTITION OF test.tbl
		FOR VALUES FROM (0) TO (10000)
		WITH (parallel_workers = 0);
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
	SELECT injection_points_attach('reindex_relation_concurrently_before_set_dead', 'wait');
	SELECT injection_points_attach('reindex_relation_concurrently_before_swap', 'wait');
}
step s3_start_reindex			{ REINDEX INDEX CONCURRENTLY test.tbl_partition_pkey; }

session s4
step s4_wakeup_to_swap		{
	SELECT injection_points_wakeup('reindex_relation_concurrently_before_swap');
	SELECT injection_points_detach('reindex_relation_concurrently_before_swap');
}
step s4_wakeup_s1		{
	SELECT injection_points_wakeup('check_exclusion_or_unique_constraint_no_conflict');
	SELECT injection_points_detach('check_exclusion_or_unique_constraint_no_conflict');
}
step s4_wakeup_s2		{
	SELECT injection_points_wakeup('exec_insert_before_insert_speculative');
	SELECT injection_points_detach('exec_insert_before_insert_speculative');
}
step s4_wakeup_to_set_dead		{
	SELECT injection_points_wakeup('reindex_relation_concurrently_before_set_dead');
	SELECT injection_points_detach('reindex_relation_concurrently_before_set_dead');
}

permutation
	s3_start_reindex
	s1_start_upsert
	s4_wakeup_to_swap
	s2_start_upsert
	s4_wakeup_s1
	s4_wakeup_s2
	s4_wakeup_to_set_dead

permutation
	s3_start_reindex
	s2_start_upsert
	s4_wakeup_to_swap
	s1_start_upsert
	s4_wakeup_s1
	s4_wakeup_s2
	s4_wakeup_to_set_dead

permutation
	s3_start_reindex
	s4_wakeup_to_swap
	s1_start_upsert
	s2_start_upsert
	s4_wakeup_s1
	s4_wakeup_to_set_dead
	s4_wakeup_s2