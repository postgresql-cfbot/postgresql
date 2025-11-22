CREATE TEMP VARIABLE temp_var01 AS int;

-- should not be accessible without variable's fence
-- should fail
SELECT temp_var01;

-- should be ok
SELECT VARIABLE(temp_var01);

-- should not crash
DO $$
BEGIN
  RAISE NOTICE '%', VARIABLE(temp_var01);
END;
$$;

-- variables cannot be used by persistent objects
-- that checks dependency
-- should fail
CREATE TEMP VIEW tempv AS SELECT VARIABLE(temp_var01);

CREATE OR REPLACE FUNCTION testvar_sql()
RETURNS int AS $$
SELECT VARIABLE(temp_var01);
$$ LANGUAGE sql;

SELECT testvar_sql();

-- session variable cannot be used as parameter of CALL or EXECUTE
CREATE OR REPLACE PROCEDURE testvar_proc(int)
AS $$
BEGIN
  RAISE NOTICE '%', $1;
END;
$$ LANGUAGE plpgsql;

-- should not crash
CALL testvar_proc(VARIABLE(temp_var01));

PREPARE prepstmt(int) AS SELECT $1;

-- should not crash
EXECUTE prepstmt(VARIABLE(temp_var01));

DROP PROCEDURE testvar_proc;
DEALLOCATE prepstmt;

CREATE ROLE regress_session_variable_test_role_03;

CREATE OR REPLACE FUNCTION testvar_sd()
RETURNS void AS $$
BEGIN
  RAISE NOTICE '%', VARIABLE(temp_var01);
END;
$$ LANGUAGE plpgsql;

-- only owner can read data
SET ROLE TO regress_session_variable_test_role_03;

-- should fail
SELECT VARIABLE(temp_var01);

-- fx with security definer should be ok
SELECT testvar_sd();

SET ROLE TO default;

DROP VARIABLE temp_var01;

-- there is not plan cache invalidation
-- but still functions that uses dropped variables
-- should not to crash

SELECT testvar_sd();
SELECT testvar_sql();

DROP FUNCTION testvar_sql();
DROP FUNCTION testvar_sd();

DROP ROLE regress_session_variable_test_role_03;

CREATE TABLE testvar_testtab(a int);
CREATE TEMP VARIABLE temp_var02 AS int;

INSERT INTO testvar_testtab SELECT * FROM generate_series(1,1000);

CREATE INDEX testvar_testtab_a ON testvar_testtab(a);

ANALYZE testvar_testtab;

-- force index
SET enable_seqscan TO OFF;

-- index scan should be used
EXPLAIN (COSTS OFF) SELECT * FROM testvar_testtab WHERE a = VARIABLE(temp_var02);

DROP INDEX testvar_testtab_a;

SET enable_seqscan TO DEFAULT;

-- parallel execution should be blocked
-- Encourage use of parallel plans
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;

-- parallel plan should be used
EXPLAIN (COSTS OFF) SELECT * FROM testvar_testtab WHERE a = 100;

-- parallel plan should not be used
EXPLAIN (COSTS OFF) SELECT * FROM testvar_testtab WHERE a = VARIABLE(temp_var02);

RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;

DROP TABLE testvar_testtab;
DROP VARIABLE temp_var02;
