CREATE EXTENSION test_dbgapi;

/*
 * There is lot of shadowed variables "v". The test should to
 * choose the variable "v" from namespace that is assigned to
 * current statement, and show the value before and after an
 * execution of any statement.
 */
CREATE OR REPLACE FUNCTION trace_variable_test_func(v int)
RETURNS void AS $$
BEGIN
  v := v + 1;

<<b1>>
  DECLARE
    v int DEFAULT trace_variable_test_func.v + 100;
  BEGIN
    v := v + 1;
    FOR v IN v + 10 .. v + 13
    LOOP
      RAISE NOTICE 'v = %', v;
    END LOOP;
  END;
END;
$$ LANGUAGE plpgsql;

/*
 * Prepare simple tracer
 */
SELECT trace_plpgsql(true);

SET test_dbgapi.trace_variable = 'v';
SET test_dbgapi.trace_forloop_variable = ON;

SELECT trace_variable_test_func(10);

