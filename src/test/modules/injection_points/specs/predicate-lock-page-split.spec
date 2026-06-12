# Test for race condition in PredicateLockPageSplit
#
# When SetNewSxactGlobalXmin() temporarily sets SxactGlobalXmin to
# InvalidTransactionId, a concurrent PredicateLockPageSplit() can see
# the invalid value and skip transferring SIREAD locks to the new page.
# This lets a third transaction insert onto the new page without SSI
# detecting the rw-conflict: s1 and s3 both compute max(id) + 1 from
# the same snapshot and insert the same id twice, which no serial
# ordering of the transactions could produce.
#
setup
{
    CREATE EXTENSION IF NOT EXISTS injection_points;
    DROP TABLE IF EXISTS test_table;

    CREATE TABLE test_table (id int);
    CREATE INDEX test_table_id_idx ON test_table USING btree (id);
    INSERT INTO test_table VALUES (1);
}

teardown
{
    DROP TABLE IF EXISTS test_table;
    DROP EXTENSION IF EXISTS injection_points;
}

session s0
step bump_xmin {
    DO $$
    BEGIN
        PERFORM pg_current_xact_id();
    END;
    $$;
}
step verify {
    SELECT id, count(*) FROM test_table GROUP BY id ORDER BY id;
}

session s1
setup {
    SELECT injection_points_set_local();
    SELECT injection_points_attach('predicate-set-sxact-global-xmin-invalid', 'wait');
}
step s1_begin {
    BEGIN ISOLATION LEVEL SERIALIZABLE;
    SELECT max(id) > 0 AS locked FROM test_table;
}
step s1_insert {
    INSERT INTO test_table
    SELECT max(id) + 1
    FROM test_table;
}
step s1_commit_wait_in_SetNewSxactGlobalXmin {
    COMMIT;
}

session s2
setup {
    SELECT injection_points_set_local();
    SELECT injection_points_attach('predicate-lock-page-split', 'wait');
}
step s2_begin {
    BEGIN ISOLATION LEVEL SERIALIZABLE;
    SELECT max(id) > 0 AS locked FROM test_table;
}
step s2_insert_wait_at_page_split {
    DO $$
    DECLARE
        next_id int := 0;
        base_size bigint := pg_relation_size('test_table_id_idx');
    BEGIN
        LOOP
            INSERT INTO test_table VALUES (next_id);
            EXIT WHEN pg_relation_size('test_table_id_idx') > base_size;
            next_id := next_id - 1;
        END LOOP;
    END;
    $$;
}
step s2_commit {
    COMMIT;
}

session s3
step s3_begin {
    BEGIN ISOLATION LEVEL SERIALIZABLE;
    SELECT max(id) > 0 AS locked FROM test_table;
}
step s3_insert {
    INSERT INTO test_table
    SELECT max(id) + 1
    FROM test_table;
}
step s3_commit {
    COMMIT;
}

session s4
step wakeup_s2_then_s1 {
    SELECT injection_points_wakeup('predicate-lock-page-split');
    SELECT injection_points_wakeup('predicate-set-sxact-global-xmin-invalid');
}

# s1_begin: s1 reads from the table, establishing SIREAD locks on the index
# bump_xmin: advance xmin so s2/s3 get a higher xmin than s1
# s2_begin, s3_begin: s2 and s3 read from the table (same snapshot as s1)
#
# s1_insert: s1 inserts max(id)+1 = 2
# s2_insert_wait_at_page_split: s2 inserts descending values until a real
#   btree page split happens, then waits in PredicateLockPageSplit before
#   checking SxactGlobalXmin.  The values must be descending so that the
#   split moves ids 1 and 2 to the new page
# s1_commit_wait_in_SetNewSxactGlobalXmin: after s2 is already waiting,
#   s1 commits and waits after SetNewSxactGlobalXmin sets SxactGlobalXmin
#   to InvalidTransactionId
# wakeup_s2_then_s1: wake s2 (sees InvalidTransactionId, skips SIREAD
#   lock transfer), then wake s1
# s3_insert: s3 inserts max(id)+1 = 2, computed from its snapshot
# s3_commit: s3 commits (should have been aborted by SSI)
# s2_commit: s2 aborts due to serialization failure
permutation
    s1_begin
    bump_xmin
    s2_begin
    s3_begin
    s1_insert
    s2_insert_wait_at_page_split
    s1_commit_wait_in_SetNewSxactGlobalXmin
    wakeup_s2_then_s1(s2_insert_wait_at_page_split,s1_commit_wait_in_SetNewSxactGlobalXmin)
    s3_insert
    s3_commit
    s2_commit
    verify
