setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA test;
	CREATE TABLE test.tbl(i int primary key, updated_at timestamp);
	CREATE UNIQUE INDEX tbl_pkey_not_safe ON test.tbl(ABS(i)) WHERE MOD(i, 2) = 0;
}

teardown
{
	DROP SCHEMA test CASCADE;
	DROP EXTENSION injection_points;
}

session test
setup	{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ReindexRelationConcurrently_index_safe', 'error');
}
step reindex	{ REINDEX INDEX CONCURRENTLY test.tbl_pkey_not_safe; }

permutation
	reindex