setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA test;
	CREATE UNLOGGED TABLE test.tbl(i int primary key, n int);
	CREATE INDEX tbl_n_idx ON test.tbl(n);
	INSERT INTO test.tbl VALUES(42,1);
}

teardown
{
	DROP SCHEMA test CASCADE;
	DROP EXTENSION injection_points;
}

session s1
setup	{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('check_exclusion_or_unique_constraint_no_conflict', 'error');
	SELECT injection_points_attach('check_exclusion_or_unique_constraint_before_index_scan', 'wait');
	SELECT injection_points_attach('index_getnext_slot_before_fetch', 'wait');
}

step s1_s1	{ INSERT INTO test.tbl VALUES(42, 1) on conflict(i) do update set n = EXCLUDED.n + 1; }

session s2
step s2_s1	{ UPDATE test.tbl SET n = n + 1 WHERE i = 42; }

session s3
step s3_s1		{
	SELECT injection_points_wakeup('check_exclusion_or_unique_constraint_before_index_scan');
	SELECT injection_points_detach('check_exclusion_or_unique_constraint_before_index_scan');
}
step s3_s2		{
	SELECT injection_points_wakeup('index_getnext_slot_before_fetch');
	SELECT injection_points_detach('index_getnext_slot_before_fetch');
}

permutation
	s1_s1
	s3_s1
	s2_s1
	s3_s2