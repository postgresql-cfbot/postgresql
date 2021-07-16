CREATE EXTENSION test_hooks;

SELECT get_times_processed_interrupts() > 0;

SELECT test_hold_interrupts_hook();

SELECT set_exit_deferred(true);
SELECT pg_terminate_backend(pg_backend_pid());
SELECT 1;
SELECT set_exit_deferred(false);
SELECT 1;

SELECT pg_backend_pid();
SELECT pg_terminate_backend(pg_backend_pid());
SELECT pg_backend_pid();
SELECT 1;
