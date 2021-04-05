setup
{
    CREATE TABLE test_stat_oid(name text NOT NULL, oid oid);

    CREATE TABLE test_stat_tab(id serial NOT NULL);
    INSERT INTO test_stat_tab DEFAULT VALUES;
    INSERT INTO test_stat_oid(name, oid) VALUES('test_stat_tab', 'test_stat_tab'::regclass);

    CREATE FUNCTION test_stat_func() RETURNS VOID LANGUAGE plpgsql AS $$BEGIN END;$$;
    INSERT INTO test_stat_oid(name, oid) VALUES('test_stat_func', 'test_stat_func'::regproc);

    CREATE FUNCTION test_stat_func2() RETURNS VOID LANGUAGE plpgsql AS $$BEGIN END;$$;
    INSERT INTO test_stat_oid(name, oid) VALUES('test_stat_func2', 'test_stat_func2'::regproc);
}

teardown
{
    DROP TABLE test_stat_oid;

    DROP TABLE IF EXISTS test_stat_tab;
    DROP FUNCTION IF EXISTS test_stat_func();
    DROP FUNCTION IF EXISTS test_stat_func2();
}

session "s1"
setup { SET stats_fetch_consistency = 'none'; }
step "s1_track_funcs_all" { SET track_functions = 'all'; }
step "s1_track_funcs_none" { SET track_functions = 'none'; }
step "s1_fetch_consistency_none" { SET stats_fetch_consistency = 'none'; }
step "s1_fetch_consistency_cache" { SET stats_fetch_consistency = 'cache'; }
step "s1_fetch_consistency_snapshot" { SET stats_fetch_consistency = 'snapshot'; }
step "s1_begin" { BEGIN; }
step "s1_commit" { COMMIT; }
step "s1_rollback" { ROLLBACK; }
step "s1_prepare_a" { PREPARE TRANSACTION 'a'; }
step "s1_commit_prepared_a" { COMMIT PREPARED 'a'; }
step "s1_rollback_prepared_a" { ROLLBACK PREPARED 'a'; }
step "s1_ff" { SELECT pg_stat_force_next_flush(); }
step "s1_func_call" { SELECT test_stat_func(); }
step "s1_func_drop" { DROP FUNCTION test_stat_func(); }
step "s1_func_stats_reset" { SELECT pg_stat_reset_single_function_counters('test_stat_func'::regproc); }
step "s1_reset" { SELECT pg_stat_reset(); }
step "s1_func_stats" {
    SELECT
        tso.name,
        pg_stat_get_function_calls(tso.oid),
        pg_stat_get_function_total_time(tso.oid) > 0 total_above_zero,
        pg_stat_get_function_self_time(tso.oid) > 0 self_above_zero
    FROM test_stat_oid AS tso
    WHERE tso.name = 'test_stat_func'
}
step "s1_func_stats2" {
    SELECT
        tso.name,
        pg_stat_get_function_calls(tso.oid),
        pg_stat_get_function_total_time(tso.oid) > 0 total_above_zero,
        pg_stat_get_function_self_time(tso.oid) > 0 self_above_zero
    FROM test_stat_oid AS tso
    WHERE tso.name = 'test_stat_func2'
}
#step "s1_func_stats_debug" {SELECT * FROM pg_stat_user_functions;}

session "s2"
setup { SET stats_fetch_consistency = 'none'; }
step "s2_track_funcs_all" { SET track_functions = 'all'; }
step "s2_track_funcs_none" { SET track_functions = 'none'; }
step "s2_begin" { BEGIN; }
step "s2_commit" { COMMIT; }
step "s2_commit_prepared_a" { COMMIT PREPARED 'a'; }
step "s2_rollback_prepared_a" { ROLLBACK PREPARED 'a'; }
step "s2_ff" { SELECT pg_stat_force_next_flush(); }
step "s2_func_call" { SELECT test_stat_func() }
step "s2_func_call2" { SELECT test_stat_func2() }
step "s2_func_stats" {
    SELECT
        tso.name,
        pg_stat_get_function_calls(tso.oid),
        pg_stat_get_function_total_time(tso.oid) > 0 total_above_zero,
        pg_stat_get_function_self_time(tso.oid) > 0 self_above_zero
    FROM test_stat_oid AS tso
    WHERE tso.name = 'test_stat_func'
}


######################
# Function stats tests
######################

# check that stats are collected iff enabled
permutation
  "s1_track_funcs_none" "s1_func_stats" "s1_func_call" "s1_func_call" "s1_ff" "s1_func_stats"
permutation
  "s1_track_funcs_all" "s1_func_stats" "s1_func_call" "s1_func_call" "s1_ff" "s1_func_stats"

# multiple function calls are accurately reported, across separate connections
permutation
  "s1_track_funcs_all" "s2_track_funcs_all" "s1_func_stats" "s2_func_stats"
  "s1_func_call" "s2_func_call" "s1_func_call" "s2_func_call" "s2_func_call" "s1_ff" "s2_ff" "s1_func_stats" "s2_func_stats"
permutation
  "s1_track_funcs_all" "s2_track_funcs_all" "s1_func_stats" "s2_func_stats"
  "s1_func_call" "s1_ff" "s2_func_call" "s2_func_call" "s2_ff" "s1_func_stats" "s2_func_stats"
permutation
  "s1_track_funcs_all" "s2_track_funcs_all" "s1_func_stats" "s2_func_stats"
  "s1_begin" "s1_func_call" "s1_func_call" "s1_commit" "s1_ff" "s1_func_stats" "s2_func_stats"


# Check interaction between dropping and stats reporting

# dropping a table remove stats iff committed
permutation
  "s1_track_funcs_all" "s2_track_funcs_all" "s1_func_stats" "s2_func_stats"
  "s1_begin" "s1_func_call" "s2_func_call" "s1_func_drop" "s2_func_call" "s2_ff" "s2_func_stats" "s1_commit" "s1_ff" "s1_func_stats" "s2_func_stats"
permutation
  "s1_track_funcs_all" "s2_track_funcs_all" "s1_func_stats" "s2_func_stats"
  "s1_begin" "s1_func_call" "s2_func_call" "s1_func_drop" "s2_func_call" "s2_ff" "s2_func_stats" "s1_rollback" "s1_ff" "s1_func_stats" "s2_func_stats"

# Verify that pending stats from before a drop do not lead to
# "reviving" stats for a dropped object
permutation
  "s1_track_funcs_all" "s2_track_funcs_all"
  "s2_func_call" "s2_ff" # this access increments refcount, preventing the shared entry from being dropped
  "s2_begin" "s2_func_call" "s1_func_drop" "s1_func_stats" "s2_commit" "s2_ff" "s1_func_stats" "s2_func_stats"
permutation
  "s1_track_funcs_all" "s2_track_funcs_all"
  "s2_begin" "s2_func_call" "s1_func_drop" "s1_func_stats" "s2_commit" "s2_ff" "s1_func_stats" "s2_func_stats"
permutation
  "s1_track_funcs_all" "s2_track_funcs_all"
  "s1_func_call" "s2_begin" "s2_func_call" "s1_func_drop" "s2_func_call" "s2_commit" "s2_ff" "s1_func_stats" "s2_func_stats"

# FIXME: this shows the bug that stats will be revived, because the
# shared stats in s2 is only referenced *after* the DROP FUNCTION
# committed. That's only possible because there is no locking (and
# thus no stats invalidation) around function calls.
permutation
  "s1_track_funcs_all" "s2_track_funcs_none"
  "s1_func_call" "s2_begin" "s2_func_call" "s1_func_drop" "s2_track_funcs_all" "s2_func_call" "s2_commit" "s2_ff" "s1_func_stats" "s2_func_stats"

# test pg_stat_reset_single_function_counters
permutation
  "s1_track_funcs_all" "s2_track_funcs_all"
  "s1_func_call"
  "s2_func_call"
  "s2_func_call2"
  "s1_ff" "s2_ff"
  "s1_func_stats"
  "s2_func_call" "s2_func_call2" "s2_ff"
  "s1_func_stats" "s1_func_stats2" "s1_func_stats"
  "s1_func_stats_reset"
  "s1_func_stats" "s1_func_stats2" "s1_func_stats"

# test pg_stat_reset
permutation
  "s1_track_funcs_all" "s2_track_funcs_all"
  "s1_func_call"
  "s2_func_call"
  "s2_func_call2"
  "s1_ff" "s2_ff"
  "s1_func_stats" "s1_func_stats2" "s1_func_stats"
  "s1_reset"
  "s1_func_stats" "s1_func_stats2" "s1_func_stats"


# Check the different snapshot consistency models

# First just some dead-trivial test verifying each model doesn't crash
permutation
  "s1_track_funcs_all" "s1_fetch_consistency_none" "s1_func_call" "s1_ff" "s1_func_stats"
permutation
  "s1_track_funcs_all" "s1_fetch_consistency_cache" "s1_func_call" "s1_ff" "s1_func_stats"
permutation
  "s1_track_funcs_all" "s1_fetch_consistency_snapshot" "s1_func_call" "s1_ff" "s1_func_stats"

# with stats_fetch_consistency=none s1 should see flushed changes in s2, despite being in a transaction
permutation
  "s1_track_funcs_all" "s2_track_funcs_all"
  "s1_fetch_consistency_none"
  "s2_func_call" "s2_ff"
  "s1_begin"
  "s1_func_stats"
  "s2_func_call" "s2_ff"
  "s1_func_stats"
  "s1_commit"

# with stats_fetch_consistency=cache s1 should not see concurrent
# changes to the same object after the first access, but a separate
# object should show changes
permutation
  "s1_track_funcs_all" "s2_track_funcs_all"
  "s1_fetch_consistency_cache"
  "s2_func_call" "s2_func_call2" "s2_ff"
  "s1_begin"
  "s1_func_stats"
  "s2_func_call" "s2_func_call2" "s2_ff"
  "s1_func_stats" "s1_func_stats2"
  "s1_commit"

# with stats_fetch_consistency=snapshot s1 should not see any
# concurrent changes after the first access
permutation
  "s1_track_funcs_all" "s2_track_funcs_all"
  "s1_fetch_consistency_snapshot"
  "s2_func_call" "s2_func_call2" "s2_ff"
  "s1_begin"
  "s1_func_stats"
  "s2_func_call" "s2_func_call2" "s2_ff"
  "s1_func_stats" "s1_func_stats2"
  "s1_commit"


# Check 2PC handling of stat drops

# S1 prepared, S1 commits prepared
permutation
  "s1_track_funcs_all" "s2_track_funcs_all"
  "s1_begin"
  "s1_func_call"
  "s2_func_call"
  "s1_func_drop"
  "s2_func_call"
  "s2_ff"
  "s1_prepare_a"
  "s2_func_call"
  "s2_ff"
  "s1_func_call"
  "s1_ff"
  "s1_func_stats"
  "s1_commit_prepared_a"
  "s1_func_stats"

# S1 prepared, S1 aborts prepared
permutation
  "s1_track_funcs_all" "s2_track_funcs_all"
  "s1_begin"
  "s1_func_call"
  "s2_func_call"
  "s1_func_drop"
  "s2_func_call"
  "s2_ff"
  "s1_prepare_a"
  "s2_func_call"
  "s2_ff"
  "s1_func_call"
  "s1_ff"
  "s1_func_stats"
  "s1_rollback_prepared_a"
  "s1_func_stats"

# S1 prepares, S2 commits prepared
permutation
  "s1_track_funcs_all" "s2_track_funcs_all"
  "s1_begin"
  "s1_func_call"
  "s2_func_call"
  "s1_func_drop"
  "s2_func_call"
  "s2_ff"
  "s1_prepare_a"
  "s2_func_call"
  "s2_ff"
  "s1_func_call"
  "s1_ff"
  "s1_func_stats"
  "s2_commit_prepared_a"
  "s1_func_stats"

# S1 prepared, S2 aborts prepared
permutation
  "s1_track_funcs_all" "s2_track_funcs_all"
  "s1_begin"
  "s1_func_call"
  "s2_func_call"
  "s1_func_drop"
  "s2_func_call"
  "s2_ff"
  "s1_prepare_a"
  "s2_func_call"
  "s2_ff"
  "s1_func_call"
  "s1_ff"
  "s1_func_stats"
  "s2_rollback_prepared_a"
  "s1_func_stats"
