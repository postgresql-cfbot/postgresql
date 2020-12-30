# This test provokes a "snapshot too old" error using a cursor.
#
# Expects old_snapshot_threshold = 10.  Not suitable for installcheck since it
# messes with internal snapmgr.c state.

setup
{
    CREATE EXTENSION IF NOT EXISTS test_sto;
    SELECT test_sto_reset_all_state();
    CREATE TABLE sto1 (c int NOT NULL);
    INSERT INTO sto1 SELECT generate_series(1, 1000);
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
step "s1decl"	{ DECLARE cursor1 CURSOR FOR SELECT c FROM sto1; }
step "s1f1"		{ FETCH FIRST FROM cursor1; }
step "s1f2"		{ FETCH FIRST FROM cursor1; }
step "s1f3"		{ FETCH FIRST FROM cursor1; }
teardown		{ COMMIT; }

session "s2"
step "s2u"		{ UPDATE sto1 SET c = 1001 WHERE c = 1; }

session "time"
step "t00"		{ SELECT test_sto_clobber_snapshot_timestamp('3000-01-01 00:00:00Z'); }
step "t10"		{ SELECT test_sto_clobber_snapshot_timestamp('3000-01-01 00:10:00Z'); }
step "t20"		{ SELECT test_sto_clobber_snapshot_timestamp('3000-01-01 00:20:00Z'); }

# if there's an update, we get a snapshot too old error at time 00:20 (not before,
# because we need page pruning to see the xmin level change from 10 minutes earlier)
permutation "t00" "s1decl" "s1f1" "t10" "s2u" "s1f2" "t20" "s1f3"

# if there's no update, no snapshot too old error at time 00:20
permutation "t00" "s1decl" "s1f1" "t10"       "s1f2" "t20" "s1f3"
