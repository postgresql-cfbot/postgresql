# This test is like sto_using_select, except that we test access via a
# hash index.  Explicit vacuuming is required in this version because
# there is are no incidental calls to heap_page_prune_opt() that can
# call SetOldSnapshotThresholdTimestamp().

setup
{
	CREATE EXTENSION IF NOT EXISTS test_sto;
	SELECT test_sto_reset_all_state();
    CREATE TABLE sto1 (c int NOT NULL);
    INSERT INTO sto1 SELECT generate_series(1, 1000);
    CREATE INDEX idx_sto1 ON sto1 USING HASH (c);
}
setup
{
    VACUUM ANALYZE sto1;
}

teardown
{
    DROP TABLE sto1;
	SELECT test_sto_reset_all_state();
}

session "s1"
setup			{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step "noseq"	{ SET enable_seqscan = false; }
step "s1f1"		{ SELECT c FROM sto1 where c = 1000; }
step "s1f2"		{ SELECT c FROM sto1 where c = 1001; }
step "s1f3"		{ SELECT c FROM sto1 where c = 1001; }
teardown		{ ROLLBACK; }

session "s2"
step "s2u"		{ UPDATE sto1 SET c = 1001 WHERE c = 1000; }
step "s2v1"		{ VACUUM sto1; }
step "s2v2"		{ VACUUM sto1; }

session "time"
step "t00"		{ SELECT test_sto_clobber_snapshot_timestamp('3000-01-01 00:00:00Z'); }
step "t10"		{ SELECT test_sto_clobber_snapshot_timestamp('3000-01-01 00:10:00Z'); }
step "t22"		{ SELECT test_sto_clobber_snapshot_timestamp('3000-01-01 00:22:00Z'); }

# snapshot too old at t22
permutation "t00" "noseq" "s1f1" "t10" "s2u" "s2v1" "s1f2" "t22" "s2v2" "s1f3"
