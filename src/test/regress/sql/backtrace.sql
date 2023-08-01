--
-- pg_log_backtrace()
--
-- Backtraces are logged to stderr and not returned to the function.
-- Furthermore, their contents can vary depending on the timing. However,
-- we can at least verify that the code doesn't fail, and that the
-- permissions are set properly.
--

SELECT pg_log_backtrace(pg_backend_pid());

SELECT pg_log_backtrace(pid) FROM pg_stat_activity
  WHERE backend_type = 'checkpointer';

CREATE ROLE regress_log_backtrace;

SELECT has_function_privilege('regress_log_backtrace',
  'pg_log_backtrace(integer)', 'EXECUTE'); -- no

GRANT EXECUTE ON FUNCTION pg_log_backtrace(integer)
  TO regress_log_backtrace;

SELECT has_function_privilege('regress_log_backtrace',
  'pg_log_backtrace(integer)', 'EXECUTE'); -- yes

SET ROLE regress_log_backtrace;
SELECT pg_log_backtrace(pg_backend_pid());
RESET ROLE;

REVOKE EXECUTE ON FUNCTION pg_log_backtrace(integer)
  FROM regress_log_backtrace;

DROP ROLE regress_log_backtrace;
