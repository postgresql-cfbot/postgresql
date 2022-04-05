--
-- Test Statistics Collector
--
-- Must be run after tenk2 has been created (by create_table),
-- populated (by create_misc) and indexed (by create_index).
--

-- conditio sine qua non
SHOW track_counts;  -- must be on

-- prepare and fill the pg_stat_toast table now:
SHOW track_toast;
SET track_toast TO on;
SHOW track_toast;
TABLE pg_stat_toast; -- view exists

CREATE TABLE toast_test (cola TEXT, colb TEXT, colc TEXT , cold TEXT);
ALTER TABLE toast_test ALTER colb SET STORAGE EXTERNAL;
ALTER TABLE toast_test ALTER colc SET STORAGE MAIN;
ALTER TABLE toast_test ALTER cold SET STORAGE PLAIN;
INSERT INTO toast_test VALUES (repeat(md5('a'),100), repeat(md5('a'),100), repeat(md5('a'),100), repeat(md5('a'),100) );
-- make sure we don't interfere with the other tests:
SET track_toast TO off;

-- ensure that both seqscan and indexscan plans are allowed
SET enable_seqscan TO on;
SET enable_indexscan TO on;
-- for the moment, we don't want index-only scans here
SET enable_indexonlyscan TO off;

-- save counters
CREATE TABLE prevstats AS
SELECT t.seq_scan, t.seq_tup_read, t.idx_scan, t.idx_tup_fetch,
       (b.heap_blks_read + b.heap_blks_hit) AS heap_blks,
       (b.idx_blks_read + b.idx_blks_hit) AS idx_blks,
       pg_stat_get_snapshot_timestamp() as snap_ts
  FROM pg_catalog.pg_stat_user_tables AS t,
       pg_catalog.pg_statio_user_tables AS b
 WHERE t.relname='tenk2' AND b.relname='tenk2';

-- function to wait for counters to advance
create function wait_for_stats() returns void as $$
declare
  start_time timestamptz := clock_timestamp();
  updated1 bool;
  updated2 bool;
  updated3 bool;
  updated4 bool;
begin
  -- We don't want to wait forever.  No timeout suffices if the OS drops our
  -- stats traffic because an earlier test file left a full UDP buffer.
  -- Hence, don't use PG_TEST_TIMEOUT_DEFAULT, which may be large for
  -- can't-happen timeouts.  Exit after 30 seconds.
  for i in 1 .. 300 loop

    -- With parallel query, the seqscan and indexscan on tenk2 might be done
    -- in parallel worker processes, which will send their stats counters
    -- asynchronously to what our own session does.  So we must check for
    -- those counts to be registered separately from the update counts.

    -- check to see if seqscan has been sensed
    SELECT (st.seq_scan >= pr.seq_scan + 1) INTO updated1
      FROM pg_stat_user_tables AS st, pg_class AS cl, prevstats AS pr
     WHERE st.relname='tenk2' AND cl.relname='tenk2';

    -- check to see if indexscan has been sensed
    SELECT (st.idx_scan >= pr.idx_scan + 1) INTO updated2
      FROM pg_stat_user_tables AS st, pg_class AS cl, prevstats AS pr
     WHERE st.relname='tenk2' AND cl.relname='tenk2';

    -- check to see if all updates have been sensed
    SELECT (n_tup_ins > 0) INTO updated3
      FROM pg_stat_user_tables WHERE relname='trunc_stats_test4';

    -- We must also check explicitly that pg_stat_get_snapshot_timestamp has
    -- advanced, because that comes from the global stats file which might
    -- be older than the per-DB stats file we got the other values from.
    SELECT (pr.snap_ts < pg_stat_get_snapshot_timestamp()) INTO updated4
      FROM prevstats AS pr;

    exit when updated1 and updated2 and updated3 and updated4;

    -- wait a little
    perform pg_sleep_for('100 milliseconds');

    -- reset stats snapshot so we can test again
    perform pg_stat_clear_snapshot();

  end loop;

  -- report time waited in postmaster log (where it won't change test output)
  raise log 'wait_for_stats delayed % seconds',
    extract(epoch from clock_timestamp() - start_time);
end
$$ language plpgsql;

-- test effects of TRUNCATE on n_live_tup/n_dead_tup counters
CREATE TABLE trunc_stats_test(id serial);
CREATE TABLE trunc_stats_test1(id serial, stuff text);
CREATE TABLE trunc_stats_test2(id serial);
CREATE TABLE trunc_stats_test3(id serial, stuff text);
CREATE TABLE trunc_stats_test4(id serial);

-- check that n_live_tup is reset to 0 after truncate
INSERT INTO trunc_stats_test DEFAULT VALUES;
INSERT INTO trunc_stats_test DEFAULT VALUES;
INSERT INTO trunc_stats_test DEFAULT VALUES;
TRUNCATE trunc_stats_test;

-- test involving a truncate in a transaction; 4 ins but only 1 live
INSERT INTO trunc_stats_test1 DEFAULT VALUES;
INSERT INTO trunc_stats_test1 DEFAULT VALUES;
INSERT INTO trunc_stats_test1 DEFAULT VALUES;
UPDATE trunc_stats_test1 SET id = id + 10 WHERE id IN (1, 2);
DELETE FROM trunc_stats_test1 WHERE id = 3;

BEGIN;
UPDATE trunc_stats_test1 SET id = id + 100;
TRUNCATE trunc_stats_test1;
INSERT INTO trunc_stats_test1 DEFAULT VALUES;
COMMIT;

-- use a savepoint: 1 insert, 1 live
BEGIN;
INSERT INTO trunc_stats_test2 DEFAULT VALUES;
INSERT INTO trunc_stats_test2 DEFAULT VALUES;
SAVEPOINT p1;
INSERT INTO trunc_stats_test2 DEFAULT VALUES;
TRUNCATE trunc_stats_test2;
INSERT INTO trunc_stats_test2 DEFAULT VALUES;
RELEASE SAVEPOINT p1;
COMMIT;

-- rollback a savepoint: this should count 4 inserts and have 2
-- live tuples after commit (and 2 dead ones due to aborted subxact)
BEGIN;
INSERT INTO trunc_stats_test3 DEFAULT VALUES;
INSERT INTO trunc_stats_test3 DEFAULT VALUES;
SAVEPOINT p1;
INSERT INTO trunc_stats_test3 DEFAULT VALUES;
INSERT INTO trunc_stats_test3 DEFAULT VALUES;
TRUNCATE trunc_stats_test3;
INSERT INTO trunc_stats_test3 DEFAULT VALUES;
ROLLBACK TO SAVEPOINT p1;
COMMIT;

-- rollback a truncate: this should count 2 inserts and produce 2 dead tuples
BEGIN;
INSERT INTO trunc_stats_test4 DEFAULT VALUES;
INSERT INTO trunc_stats_test4 DEFAULT VALUES;
TRUNCATE trunc_stats_test4;
INSERT INTO trunc_stats_test4 DEFAULT VALUES;
ROLLBACK;

-- do a seqscan
SELECT count(*) FROM tenk2;
-- do an indexscan
-- make sure it is not a bitmap scan, which might skip fetching heap tuples
SET enable_bitmapscan TO off;
SELECT count(*) FROM tenk2 WHERE unique1 = 1;
RESET enable_bitmapscan;

-- We can't just call wait_for_stats() at this point, because we only
-- transmit stats when the session goes idle, and we probably didn't
-- transmit the last couple of counts yet thanks to the rate-limiting logic
-- in pgstat_report_stat().  But instead of waiting for the rate limiter's
-- timeout to elapse, let's just start a new session.  The old one will
-- then send its stats before dying.
\c -

-- wait for stats collector to update
SELECT wait_for_stats();

-- check effects
SELECT relname, n_tup_ins, n_tup_upd, n_tup_del, n_live_tup, n_dead_tup
  FROM pg_stat_user_tables
 WHERE relname like 'trunc_stats_test%' order by relname;

SELECT st.seq_scan >= pr.seq_scan + 1,
       st.seq_tup_read >= pr.seq_tup_read + cl.reltuples,
       st.idx_scan >= pr.idx_scan + 1,
       st.idx_tup_fetch >= pr.idx_tup_fetch + 1
  FROM pg_stat_user_tables AS st, pg_class AS cl, prevstats AS pr
 WHERE st.relname='tenk2' AND cl.relname='tenk2';

SELECT st.heap_blks_read + st.heap_blks_hit >= pr.heap_blks + cl.relpages,
       st.idx_blks_read + st.idx_blks_hit >= pr.idx_blks + 1
  FROM pg_statio_user_tables AS st, pg_class AS cl, prevstats AS pr
 WHERE st.relname='tenk2' AND cl.relname='tenk2';

SELECT pr.snap_ts < pg_stat_get_snapshot_timestamp() as snapshot_newer
FROM prevstats AS pr;

DROP TABLE trunc_stats_test, trunc_stats_test1, trunc_stats_test2, trunc_stats_test3, trunc_stats_test4;
DROP TABLE prevstats;

-- test BRIN index doesn't block HOT update - we include this test here, as it
-- relies on statistics collector and so it may occasionally fail, especially
-- on slower systems
CREATE TABLE brin_hot (
        id  integer PRIMARY KEY,
        val integer NOT NULL
) WITH (autovacuum_enabled = off, fillfactor = 70);

INSERT INTO brin_hot SELECT *, 0 FROM generate_series(1, 235);
CREATE INDEX val_brin ON brin_hot using brin(val);

CREATE FUNCTION wait_for_hot_stats() RETURNS void AS $$
DECLARE
        start_time timestamptz := clock_timestamp();
        updated bool;
BEGIN
        -- we don't want to wait forever; loop will exit after 30 seconds
        FOR i IN 1 .. 300 LOOP
                SELECT (pg_stat_get_tuples_hot_updated('brin_hot'::regclass::oid) > 0) INTO updated;
                EXIT WHEN updated;

                -- wait a little
                PERFORM pg_sleep_for('100 milliseconds');
                -- reset stats snapshot so we can test again
                PERFORM pg_stat_clear_snapshot();
        END LOOP;
        -- report time waited in postmaster log (where it won't change test output)
        RAISE log 'wait_for_hot_stats delayed % seconds',
          EXTRACT(epoch FROM clock_timestamp() - start_time);
END
$$ LANGUAGE plpgsql;

UPDATE brin_hot SET val = -3 WHERE id = 42;

-- We can't just call wait_for_hot_stats() at this point, because we only
-- transmit stats when the session goes idle, and we probably didn't
-- transmit the last couple of counts yet thanks to the rate-limiting logic
-- in pgstat_report_stat().  But instead of waiting for the rate limiter's
-- timeout to elapse, let's just start a new session.  The old one will
-- then send its stats before dying.
\c -

SELECT wait_for_hot_stats();
SELECT pg_stat_get_tuples_hot_updated('brin_hot'::regclass::oid);

DROP TABLE brin_hot;
DROP FUNCTION wait_for_hot_stats();

-- now check that the track_toast stuff worked:
SELECT attname
	,storagemethod
	,externalized
	,compressmethod
	,compressattempts
	,compresssuccesses
	,compressedsize < originalsize AS compression_works
	, total_time > 0 AS takes_time
FROM pg_stat_toast WHERE relname = 'toast_test' ORDER BY attname;
SELECT compressattempts=0 AS external_doesnt_compress FROM pg_stat_toast WHERE relname = 'toast_test' AND storagemethod = 'e';
SELECT externalized=0 AS main_doesnt_externalize FROM pg_stat_toast WHERE relname = 'toast_test' AND storagemethod = 'm';
DROP TABLE toast_test;
SELECT count(*) FROM pg_stat_toast WHERE relname = 'toast_test';

-- ensure that stats accessors handle NULL input correctly
SELECT pg_stat_get_replication_slot(NULL);
SELECT pg_stat_get_subscription_stats(NULL);


-- End of Stats Test
