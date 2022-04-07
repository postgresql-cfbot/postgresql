-- predictability
SET synchronous_commit = on;

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');

SELECT 'msg1' FROM pg_logical_emit_message(true, 'test', 'msg1');
SELECT 'msg2' FROM pg_logical_emit_message(false, 'test', 'msg2');

BEGIN;
SELECT 'msg3' FROM pg_logical_emit_message(true, 'test', 'msg3');
SELECT 'msg4' FROM pg_logical_emit_message(false, 'test', 'msg4');
ROLLBACK;

BEGIN;
SELECT 'msg5' FROM pg_logical_emit_message(true, 'test', 'msg5');
SELECT 'msg6' FROM pg_logical_emit_message(false, 'test', 'msg6');
SELECT 'msg7' FROM pg_logical_emit_message(true, 'test', 'msg7');
COMMIT;

SELECT 'ignorethis' FROM pg_logical_emit_message(true, 'test', 'czechtastic');

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'force-binary', '0', 'skip-empty-xacts', '1');

-- test db filtering
\set prevdb :DBNAME
\c template1

SELECT 'otherdb1' FROM pg_logical_emit_message(false, 'test', 'otherdb1');
SELECT 'otherdb2' FROM pg_logical_emit_message(true, 'test', 'otherdb2');

\c :prevdb
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'force-binary', '0', 'skip-empty-xacts', '1');

-- no data in this table, but emit logical INSERT/UPDATE/DELETE for it
CREATE TABLE dummy(i int, t text, n numeric, primary key(t));

SELECT pg_logical_emit_insert('dummy', row(1, 'one', 0.1)::dummy) <> '0/0'::pg_lsn;
SELECT pg_logical_emit_insert('dummy', row(2, 'two', 0.2)::dummy) <> '0/0'::pg_lsn;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'force-binary', '0', 'skip-empty-xacts', '1', 'include-xids', '0');

SELECT * FROM dummy;

SELECT pg_logical_emit_delete('dummy', row(12, 'twelve', 0.12)::dummy) <> '0/0'::pg_lsn;

SELECT pg_logical_emit_update('dummy', row(15, 'fifteen', 0.15)::dummy,
       row(16, 'sixteen', 0.16)::dummy) <> '0/0'::pg_lsn;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'force-binary', '0', 'skip-empty-xacts', '1', 'include-xids', '0');

ALTER TABLE dummy REPLICA IDENTITY NOTHING;

SELECT pg_logical_emit_delete('dummy', row(12, 'twelve', 0.12)::dummy) <> '0/0'::pg_lsn;

SELECT pg_logical_emit_update('dummy', row(15, 'fifteen', 0.15)::dummy,
       row(16, 'sixteen', 0.16)::dummy) <> '0/0'::pg_lsn;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'force-binary', '0', 'skip-empty-xacts', '1', 'include-xids', '0');

SELECT pg_logical_emit_truncate(ARRAY['dummy'], true, false) <> '0/0'::pg_lsn;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'force-binary', '0', 'skip-empty-xacts', '1', 'include-xids', '0');

CREATE UNLOGGED TABLE dummy_u(i int, t text, n numeric, primary key (t));
-- return invalid
SELECT pg_logical_emit_insert('dummy_u', row(7, 'seven', 0.7)::dummy_u);
-- return invalid
SELECT pg_logical_emit_update('dummy_u', row(7, 'seven', 0.7)::dummy_u,
       row(11, 'eleven', 0.11)::dummy_u);
-- return invalid
SELECT pg_logical_emit_update('dummy_u', row(NULL, 'seven', 0.7)::dummy_u,
       row(11, 'eleven', 0.11)::dummy_u);
-- error
SELECT pg_logical_emit_update('dummy_u', row(7, NULL, 0.7)::dummy_u,
       row(11, 'eleven', 0.11)::dummy_u);
-- return invalid
SELECT pg_logical_emit_delete('dummy_u', row(7, 'seven', 0.7)::dummy_u);
-- error
SELECT pg_logical_emit_delete('dummy_u', row(7, NULL, 0.7)::dummy_u);

-- return invalid
SELECT pg_logical_emit_truncate(ARRAY['dummy_u'], false, false);

SELECT pg_logical_emit_truncate(ARRAY['dummy','dummy_u'], false, true) <> '0/0'::pg_lsn;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'force-binary', '0', 'skip-empty-xacts', '1', 'include-xids', '0');

DROP TABLE dummy;

DROP TABLE dummy_u;

SELECT 'cleanup' FROM pg_drop_replication_slot('regression_slot');
