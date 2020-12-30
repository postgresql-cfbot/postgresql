# This test provokes a "snapshot too old" error using SELECT statements.
#
# Expects old_snapshot_threshold = 10.  Not suitable for installcheck since it
# messes with internal snapmgr.c state.

setup
{
	CREATE EXTENSION IF NOT EXISTS test_sto;
	CREATE EXTENSION IF NOT EXISTS pg_visibility;
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
step "s1f1"		{ SELECT c FROM sto1 ORDER BY c LIMIT 1; }
step "s1f2"		{ SELECT c FROM sto1 ORDER BY c LIMIT 1; }
step "s1f3"		{ SELECT c FROM sto1 ORDER BY c LIMIT 1; }
step "s1f4"		{ SELECT c FROM sto1 ORDER BY c LIMIT 1; }
teardown		{ COMMIT; }

session "s2"
step "s2vis1"	{ SELECT EVERY(all_visible) FROM pg_visibility_map('sto1'::regclass); }
step "s2u"		{ UPDATE sto1 SET c = 1001 WHERE c = 1; }
step "s2vis2"	{ SELECT EVERY(all_visible) FROM pg_visibility_map('sto1'::regclass); }
step "s2vac1"	{ VACUUM sto1; }
step "s2vis3"	{ SELECT EVERY(all_visible) FROM pg_visibility_map('sto1'::regclass); }
step "s2vac2"	{ VACUUM sto1; }
step "s2vis4"	{ SELECT EVERY(all_visible) FROM pg_visibility_map('sto1'::regclass); }

session "time"
step "t00"		{ SELECT test_sto_clobber_snapshot_timestamp('3000-01-01 00:00:00Z'); }
step "t01"		{ SELECT test_sto_clobber_snapshot_timestamp('3000-01-01 00:01:00Z'); }
step "t10"		{ SELECT test_sto_clobber_snapshot_timestamp('3000-01-01 00:10:00Z'); }
step "t12"		{ SELECT test_sto_clobber_snapshot_timestamp('3000-01-01 00:12:00Z'); }

# If there's an update, we get a snapshot too old error at time 00:12, and
# VACUUM is allowed to remove the tuple our snapshot could see, which we know
# because we see that the relation becomes all visible.  The earlier VACUUMs
# were unable to remove the tuple we could see, which is is obvious because we
# can see the row with value 1, and from the relation not being all visible
# after the VACUUM.
permutation "t00" "s2vis1" "s1f1" "t01" "s2u" "s2vis2" "s1f2" "t10" "s2vac1" "s2vis3" "s1f3" "t12" "s1f4" "s2vac2" "s2vis4"

# Almost the same schedule, but this time we'll put s2vac2 and s2vis4 before
# s1f4 just to demonstrate that the early pruning is allowed before the error
# aborts s1's transaction.
permutation "t00" "s2vis1" "s1f1" "t01" "s2u" "s2vis2" "s1f2" "t10" "s2vac1" "s2vis3" "s1f3" "t12" "s2vac2" "s2vis4" "s1f4"

# If we run the same schedule as above but without the update, we get no
# snapshot too old error (even though our snapshot is older than the
# threshold), and the relation remains all visible.
permutation "t00" "s2vis1" "s1f1" "t01"       "s2vis2" "s1f2" "t10" "s2vac1" "s2vis3" "s1f3" "t12" "s2vac2" "s2vis4" "s1f4"
