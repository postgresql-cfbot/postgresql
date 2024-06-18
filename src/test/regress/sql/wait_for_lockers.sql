--
-- Test the pg_wait_for_lockers() function
--

-- directory paths and dlsuffix are passed to us in environment variables
\getenv libdir PG_LIBDIR
\getenv dlsuffix PG_DLSUFFIX

\set regresslib :libdir '/regress' :dlsuffix

-- Setup
CREATE TABLE wfl_tbl1 (a BIGINT);

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
