CREATE FUNCTION get_times_processed_interrupts()
RETURNS pg_catalog.int4 LANGUAGE c VOLATILE AS 'MODULE_PATHNAME';

CREATE FUNCTION set_exit_deferred(pg_catalog.bool)
RETURNS pg_catalog.void LANGUAGE c VOLATILE AS 'MODULE_PATHNAME';

CREATE FUNCTION test_hold_interrupts_hook()
RETURNS pg_catalog.void LANGUAGE c VOLATILE AS 'MODULE_PATHNAME';
