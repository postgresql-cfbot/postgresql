--
-- Test for foreign transaction management.
--

CREATE EXTENSION test_fdwxact;

-- setup one server that don't support transaction management API
CREATE SERVER srv_1 FOREIGN DATA WRAPPER test_fdw;

-- setup two servers that support only commit and rollback API
CREATE SERVER srv_no2pc_1 FOREIGN DATA WRAPPER test_no2pc_fdw;
CREATE SERVER srv_no2pc_2 FOREIGN DATA WRAPPER test_no2pc_fdw;

-- setup two servers that support commit, rollback and prepare API.
-- That is, those two server support two-phase commit protocol.
CREATE SERVER srv_2pc_1 FOREIGN DATA WRAPPER test_2pc_fdw;
CREATE SERVER srv_2pc_2 FOREIGN DATA WRAPPER test_2pc_fdw;

CREATE TABLE t (i int);
CREATE FOREIGN TABLE ft_1 (i int) SERVER srv_1;
CREATE FOREIGN TABLE ft_no2pc_1 (i int) SERVER srv_no2pc_1;
CREATE FOREIGN TABLE ft_no2pc_2 (i int) SERVER srv_no2pc_2;
CREATE FOREIGN TABLE ft_2pc_1 (i int) SERVER srv_2pc_1;
CREATE FOREIGN TABLE ft_2pc_2 (i int) SERVER srv_2pc_2;

CREATE USER MAPPING FOR PUBLIC SERVER srv_1;
CREATE USER MAPPING FOR PUBLIC SERVER srv_no2pc_1;
CREATE USER MAPPING FOR PUBLIC SERVER srv_no2pc_2;
CREATE USER MAPPING FOR PUBLIC SERVER srv_2pc_1;
CREATE USER MAPPING FOR PUBLIC SERVER srv_2pc_2;

-- function to wait for counters to advance
CREATE PROCEDURE wait_for_resolution(expected int) AS $$
DECLARE
  start_time timestamptz := clock_timestamp();
  resolved bool;
BEGIN
  -- we don't want to wait forever; loop will exit after 30 seconds
  FOR i IN 1 .. 300 LOOP

    -- check to see if all updates have been reset/updated
    SELECT count(*) = expected INTO resolved FROM pg_foreign_xacts;

    exit WHEN resolved;

    -- wait a little
    perform pg_sleep_for('100 milliseconds');

  END LOOP;

  -- report time waited in postmaster log (where it won't change test output)
  RAISE LOG 'wait_for_resolution delayed % seconds',
    extract(epoch from clock_timestamp() - start_time);
END
$$ LANGUAGE plpgsql;

-- Test 'disabled' mode.
-- Modifies one or two servers but since we don't require two-phase
-- commit, all case should not raise an error.
SET foreign_twophase_commit TO disabled;

BEGIN;
INSERT INTO ft_1 VALUES (1);
COMMIT;
BEGIN;
INSERT INTO ft_no2pc_1 VALUES (1);
COMMIT;
BEGIN;
INSERT INTO ft_no2pc_1 VALUES (1);
ROLLBACK;

BEGIN;
INSERT INTO ft_2pc_1 VALUES (1);
COMMIT;
BEGIN;
INSERT INTO ft_2pc_1 VALUES (1);
ROLLBACK;

BEGIN;
INSERT INTO ft_2pc_1 VALUES (1);
INSERT INTO ft_no2pc_2 VALUES (1);
COMMIT;

BEGIN;
INSERT INTO ft_2pc_1 VALUES (1);
INSERT INTO ft_2pc_2 VALUES (1);
COMMIT;


-- Test 'required' mode.
-- In this case, when two-phase commit is required, all servers
-- which are involved in the and modified need to support two-phase
-- commit protocol. Otherwise transaction will rollback.
SET foreign_twophase_commit TO 'required';

-- Ok. Writing only one server doesn't require two-phase commit.
BEGIN;
INSERT INTO ft_no2pc_1 VALUES (1);
COMMIT;
BEGIN;
INSERT INTO ft_2pc_1 VALUES (1);
COMMIT;

-- Ok. Writing two servers, we require two-phase commit and success.
BEGIN;
INSERT INTO ft_2pc_1 VALUES (1);
INSERT INTO ft_2pc_2 VALUES (1);
COMMIT;
BEGIN;
INSERT INTO t VALUES (1);
INSERT INTO ft_2pc_1 VALUES (1);
COMMIT;

-- Ok. Only reading servers doesn't require two-phase commit.
BEGIN;
SELECT * FROM ft_2pc_1;
SELECT * FROM ft_2pc_2;
COMMIT;
BEGIN;
SELECT * FROM ft_1;
SELECT * FROM ft_no2pc_1;
COMMIT;

-- Ok. Read one server and write one server.
BEGIN;
SELECT * FROM ft_1;
INSERT INTO ft_no2pc_1 VALUES (1);
COMMIT;
BEGIN;
SELECT * FROM ft_no2pc_1;
INSERT INTO ft_2pc_1 VALUES (1);
COMMIT;

-- Ok. only ft_2pc_1 is committed in one-phase.
BEGIN;
INSERT INTO ft_1 VALUES (1);
INSERT INTO ft_2pc_1 VALUES (1);
COMMIT;

-- Error. ft_no2pc_1 doesn't support two-phase commit.
BEGIN;
INSERT INTO ft_no2pc_1 VALUES (1);
INSERT INTO ft_2pc_1 VALUES (1);
COMMIT;

-- Error. Both ft_no2pc_1 and ft_no2pc_2 don't support two-phase
-- commit.
BEGIN;
INSERT INTO ft_no2pc_1 VALUES (1);
INSERT INTO ft_no2pc_2 VALUES (1);
COMMIT;

-- Error. Two-phase commit is required because of writes on two
-- servers: local node and ft_no2pc_1. But ft_no2pc_1 doesn't support
-- two-phase commit.
BEGIN;
INSERT INTO t VALUES (1);
INSERT INTO ft_no2pc_1 VALUES (1);
COMMIT;


-- Tests for PREPARE.
-- Prepare two transactions: local and foreign.
BEGIN;
INSERT INTO ft_2pc_1 VALUES(1);
INSERT INTO t VALUES(3);
PREPARE TRANSACTION 'global_x1';
SELECT count(*) FROM pg_foreign_xacts;
COMMIT PREPARED 'global_x1';
CALL wait_for_resolution(0);

-- Even if the transaction modified only one foreign server,
-- we prepare foreign transaction.
BEGIN;
INSERT INTO ft_2pc_1 VALUES(1);
PREPARE TRANSACTION 'global_x1';
SELECT count(*) FROM pg_foreign_xacts;
ROLLBACK PREPARED 'global_x1';
CALL wait_for_resolution(0);

-- Error. PREPARE needs all involved foreign servers to
-- support two-phsae commit.
BEGIN;
INSERT INTO ft_no2pc_1 VALUES (1);
PREPARE TRANSACTION 'global_x1';
