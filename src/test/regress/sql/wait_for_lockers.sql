--
-- Test the pg_wait_for_lockers() function
--

-- directory paths and dlsuffix are passed to us in environment variables
\getenv libdir PG_LIBDIR
\getenv dlsuffix PG_DLSUFFIX

\set regresslib :libdir '/regress' :dlsuffix

-- Setup
CREATE SCHEMA wfl_schema1;
SET search_path = wfl_schema1;
CREATE TABLE wfl_tbl1 (a BIGINT);
CREATE ROLE regress_rol_wfl1;
ALTER ROLE regress_rol_wfl1 SET search_path = wfl_schema1;
GRANT USAGE ON SCHEMA wfl_schema1 TO regress_rol_wfl1;

-- Try all valid options
BEGIN TRANSACTION;
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'AccessShareLock', FALSE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'RowShareLock', FALSE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'RowExclusiveLock', FALSE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'ShareUpdateExclusiveLock', FALSE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'ShareLock', FALSE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'ShareRowExclusiveLock', FALSE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'ExclusiveLock', FALSE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'AccessExclusiveLock', FALSE);

select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'AccessShareLock', TRUE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'RowShareLock', TRUE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'RowExclusiveLock', TRUE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'ShareUpdateExclusiveLock', TRUE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'ShareLock', TRUE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'ShareRowExclusiveLock', TRUE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'ExclusiveLock', TRUE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'AccessExclusiveLock', TRUE);
ROLLBACK;

-- pg_wait_for_lockers() does nothing if the transaction itself is the only locker
BEGIN TRANSACTION;
LOCK TABLE wfl_tbl1 IN ACCESS EXCLUSIVE MODE;
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'AccessExclusiveLock', TRUE);
ROLLBACK;

-- pg_wait_for_lockers() is allowed outside a transaction
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'AccessExclusiveLock', FALSE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'AccessExclusiveLock', TRUE);

-- pg_wait_for_lockers() requires some permissions regardless of lock mode
-- fail without permissions
SET ROLE regress_rol_wfl1;
BEGIN;
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'AccessShareLock', FALSE);
ROLLBACK;
BEGIN;
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'AccessShareLock', TRUE);
ROLLBACK;
RESET ROLE;
-- succeed with only SELECT permissions and ACCESS EXCLUSIVE mode
GRANT SELECT ON TABLE wfl_tbl1 TO regress_rol_wfl1;
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'AccessExclusiveLock', FALSE);
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'AccessExclusiveLock', TRUE);
RESET ROLE;
REVOKE SELECT ON TABLE wfl_tbl1 FROM regress_rol_wfl1;

-- fail gracefully with bogus arguments
BEGIN;
-- invalid oid
select pg_wait_for_lockers('{0}'::oid[]::regclass[], 'AccessShareLock', FALSE);
ROLLBACK;
BEGIN;
-- nonexistent oid
select pg_wait_for_lockers('{987654321}'::oid[]::regclass[], 'AccessShareLock', FALSE);
ROLLBACK;
BEGIN;
-- views are not supported
select pg_wait_for_lockers('{pg_locks}'::regclass[], 'AccessShareLock', FALSE);
ROLLBACK;
BEGIN;
-- bogus lock mode
select pg_wait_for_lockers('{wfl_tbl1}'::regclass[], 'AccessRowShareUpdateExclusiveLock', TRUE);
ROLLBACK;

--
-- Clean up
--
DROP TABLE wfl_tbl1;
DROP SCHEMA wfl_schema1 CASCADE;
DROP ROLE regress_rol_wfl1;
