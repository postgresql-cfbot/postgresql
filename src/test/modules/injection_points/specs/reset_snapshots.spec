setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA test;
	CREATE TABLE test.tbl(i int primary key, j int);
	INSERT INTO test.tbl SELECT i, i * I FROM generate_series(1, 200) s(i);

	CREATE FUNCTION test.predicate_stable(integer) RETURNS bool IMMUTABLE
									  LANGUAGE plpgsql AS $$
									  BEGIN
										EXECUTE 'SELECT txid_current()';
										RETURN MOD($1, 2) = 0;
									  END; $$;

	CREATE FUNCTION test.predicate_stable_no_param() RETURNS bool IMMUTABLE
									  LANGUAGE plpgsql AS $$
									  BEGIN
										EXECUTE 'SELECT txid_current()';
										RETURN false;
									  END; $$;
}

teardown
{
	DROP SCHEMA test CASCADE;
	DROP EXTENSION injection_points;
}

session test
setup	{
	SELECT injection_points_attach('heapam_index_validate_scan_no_xid', 'notice');
	SELECT injection_points_attach('heap_reset_scan_snapshot_effective', 'notice');
	SELECT injection_points_attach('_bt_leader_participate_as_worker', 'wait');
}
step sleep { SELECT pg_sleep(10); }
step drop_index { DROP INDEX CONCURRENTLY test.idx; }
step create_index_concurrently_simple	{ CREATE INDEX CONCURRENTLY idx ON test.tbl(i, j); }
step create_unique_index_concurrently_simple	{ CREATE UNIQUE INDEX CONCURRENTLY idx ON test.tbl(i); }
step create_index_concurrently_predicate_expression_mod	{ CREATE INDEX CONCURRENTLY idx ON test.tbl(MOD(i, 2), j) WHERE MOD(i, 2) = 0; }
step create_index_concurrently_predicate_set_xid	{ CREATE INDEX CONCURRENTLY idx ON test.tbl(i, j) WHERE test.predicate_stable(i); }
step create_index_concurrently_predicate_set_xid_no_param	{ CREATE INDEX CONCURRENTLY idx ON test.tbl(i, j) WHERE test.predicate_stable_no_param(); }
step reindex_index_concurrently { REINDEX INDEX CONCURRENTLY test.idx; }
step set_parallel_workers_1 { ALTER TABLE test.tbl SET (parallel_workers=0); }
step set_parallel_workers_2 { ALTER TABLE test.tbl SET (parallel_workers=2); }
step detach {
	SELECT injection_points_detach('heapam_index_validate_scan_no_xid');
	SELECT injection_points_detach('heap_reset_scan_snapshot_effective');
	SELECT injection_points_detach('_bt_leader_participate_as_worker');
}

session wakeup_session
step wakeup { SELECT injection_points_wakeup('_bt_leader_participate_as_worker'); }

permutation
	set_parallel_workers_1
	create_index_concurrently_simple
	reindex_index_concurrently
	drop_index
	detach

permutation
	set_parallel_workers_1
	create_unique_index_concurrently_simple
	reindex_index_concurrently
	drop_index
	detach

permutation
	set_parallel_workers_1
	create_index_concurrently_predicate_expression_mod
	reindex_index_concurrently
	drop_index
	detach

permutation
	set_parallel_workers_1
	create_index_concurrently_predicate_set_xid_no_param
	reindex_index_concurrently
	drop_index
	detach

permutation
	set_parallel_workers_1
	create_index_concurrently_predicate_set_xid
	reindex_index_concurrently
	drop_index
	detach

permutation
	set_parallel_workers_2
	create_index_concurrently_simple
	wakeup
	reindex_index_concurrently
	wakeup
	drop_index
	detach

permutation
	set_parallel_workers_2
	create_unique_index_concurrently_simple
	wakeup
	reindex_index_concurrently
	wakeup
	drop_index
	detach

permutation
	set_parallel_workers_2
	create_index_concurrently_predicate_expression_mod
	wakeup
	reindex_index_concurrently
	wakeup
	drop_index
	detach