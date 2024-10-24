# Test for checking dead_tuples, tuples_deleted and frozen tuples in pg_stat_vacuum_tables.
# Dead_tuples values are counted when vacuum cannot clean up unused tuples while lock is using another transaction.
# Dead_tuples aren't increased after releasing lock compared with tuples_deleted, which increased
# by the value of the cleared tuples that the vacuum managed to clear.

setup
{
    CREATE TABLE test_vacuum_stat_isolation(id int, ival int) WITH (autovacuum_enabled = off);
    SET track_io_timing = on;
}

teardown
{
    DROP TABLE test_vacuum_stat_isolation CASCADE;
    RESET track_io_timing;
}

session s1
step s1_begin_repeatable_read   {
  BEGIN transaction ISOLATION LEVEL REPEATABLE READ;
  select count(ival) from test_vacuum_stat_isolation where id>900;
  }
step s1_commit                  { COMMIT; }

session s2
step s2_insert                  { INSERT INTO test_vacuum_stat_isolation(id, ival) SELECT ival, ival%10 FROM generate_series(1,1000) As ival; }
step s2_update                  { UPDATE test_vacuum_stat_isolation SET ival = ival  2 where id > 900; }
step s2_delete                  { DELETE FROM test_vacuum_stat_isolation where id > 900; }
step s2_insert_interrupt        { INSERT INTO test_vacuum_stat_isolation values (1,1); }
step s2_vacuum                  { VACUUM test_vacuum_stat_isolation; }
step s2_checkpoint              { CHECKPOINT; }
step s2_print_vacuum_stats_table
{
    SELECT
    vt.relname, vt.tuples_deleted, vt.dead_tuples, vt.tuples_frozen
    FROM pg_stat_vacuum_tables vt, pg_class c
    WHERE vt.relname = 'test_vacuum_stat_isolation' AND vt.relid = c.oid;
}

permutation
    s2_insert
    s2_print_vacuum_stats_table
    s1_begin_repeatable_read
    s2_update
    s2_insert_interrupt
    s2_vacuum
    s2_print_vacuum_stats_table
    s1_commit
    s2_checkpoint
    s2_vacuum
    s2_print_vacuum_stats_table