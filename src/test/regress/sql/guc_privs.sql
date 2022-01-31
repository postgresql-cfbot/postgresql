-- Test superuser
-- Superuser DBA
CREATE ROLE regress_admin SUPERUSER;
-- Perform all operations as user 'regress_admin' --
SET SESSION AUTHORIZATION regress_admin;
-- PGC_BACKEND
SET ignore_system_indexes = OFF;  -- fail, cannot be set after connection start
RESET ignore_system_indexes;  -- fail, cannot be set after connection start
ALTER SYSTEM SET ignore_system_indexes = OFF;  -- ok
ALTER SYSTEM RESET ignore_system_indexes;  -- ok
-- PGC_INTERNAL
SET block_size = 50;  -- fail, cannot be changed
RESET block_size;  -- fail, cannot be changed
ALTER SYSTEM SET block_size = 50;  -- fail, cannot be changed
ALTER SYSTEM RESET block_size;  -- fail, cannot be changed
-- PGC_POSTMASTER
SET autovacuum_freeze_max_age = 1000050000;  -- fail, requires restart
RESET autovacuum_freeze_max_age;  -- fail, requires restart
ALTER SYSTEM SET autovacuum_freeze_max_age = 1000050000;  -- ok
ALTER SYSTEM RESET autovacuum_freeze_max_age;  -- ok
ALTER SYSTEM SET config_file = '/usr/local/data/postgresql.conf';  -- fail, cannot be changed
ALTER SYSTEM RESET config_file;  -- fail, cannot be changed
-- PGC_SIGHUP
SET autovacuum = OFF;  -- fail, requires reload
RESET autovacuum;  -- fail, requires reload
ALTER SYSTEM SET autovacuum = OFF;  -- ok
ALTER SYSTEM RESET autovacuum;  -- ok
-- PGC_SUSET
SET lc_messages = 'C';  -- ok
RESET lc_messages;  -- ok
ALTER SYSTEM SET lc_messages = 'C';  -- ok
ALTER SYSTEM RESET lc_messages;  -- ok
-- PGC_SU_BACKEND
SET jit_debugging_support = OFF;  -- fail, cannot be set after connection start
RESET jit_debugging_support;  -- fail, cannot be set after connection start
ALTER SYSTEM SET jit_debugging_support = OFF;  -- ok
ALTER SYSTEM RESET jit_debugging_support;  -- ok
-- PGC_USERSET
SET DateStyle = 'ISO, MDY';  -- ok
RESET DateStyle;  -- ok
ALTER SYSTEM SET DateStyle = 'ISO, MDY';  -- ok
ALTER SYSTEM RESET DateStyle;  -- ok
ALTER SYSTEM SET ssl_renegotiation_limit = 0;  -- fail, cannot be changed
ALTER SYSTEM RESET ssl_renegotiation_limit;  -- fail, cannot be changed
-- Finished testing superuser
RESET statement_timeout;
-- Check setting privileges prior to creating any
SELECT grantee, setting, privilege_type, is_grantable
	FROM pg_catalog.pg_setting_privileges
	ORDER BY grantee, setting, privilege_type;
-- Create non-superuser with privileges to configure host resource usage
CREATE ROLE regress_host_resource_admin NOSUPERUSER;
-- Check the new role does not yet have privileges on settings
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'SET VALUE, ALTER SYSTEM');
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'SET VALUE');
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'ALTER SYSTEM');
-- Check inappropriate and nonsense privilege types
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'SELECT, UPDATE, CREATE');
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'USAGE');
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'WHATEVER');
-- Grant privileges on settings to the new non-superuser role
GRANT SET VALUE, ALTER SYSTEM ON
	autovacuum_work_mem, hash_mem_multiplier, logical_decoding_work_mem,
	maintenance_work_mem, max_stack_depth, min_dynamic_shared_memory,
	shared_buffers, temp_buffers, temp_file_limit, work_mem
TO regress_host_resource_admin;
-- Check setting privileges after creating some
SELECT grantee, setting, privilege_type, is_grantable
	FROM pg_catalog.pg_setting_privileges
	ORDER BY grantee, setting, privilege_type;
-- Check the new role now has privilges on settings
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'SET VALUE, ALTER SYSTEM');
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'SET VALUE');
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'ALTER SYSTEM');
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'SET VALUE WITH GRANT OPTION, ALTER SYSTEM WITH GRANT OPTION');
-- Check again the inappropriate and nonsense privilege types.  The prior similar check
-- was performed before any entry for work_mem existed.
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'SELECT, UPDATE, CREATE');
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'USAGE');
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'WHATEVER');
SELECT has_setting_privilege('regress_host_resource_admin', 'work_mem', 'WHATEVER WITH GRANT OPTION');
-- Check other function signatures
SELECT has_setting_privilege('regress_host_resource_admin',
							 (SELECT oid FROM pg_catalog.pg_setting_acl WHERE setting = 'max_stack_depth'),
							 'SET VALUE');
SELECT has_setting_privilege((SELECT oid FROM pg_catalog.pg_authid WHERE rolname = 'regress_host_resource_admin'),
							 'max_stack_depth',
							 'SET VALUE');
SELECT has_setting_privilege((SELECT oid FROM pg_catalog.pg_authid WHERE rolname = 'regress_host_resource_admin'),
							 (SELECT oid FROM pg_catalog.pg_setting_acl WHERE setting = 'max_stack_depth'),
							 'SET VALUE');
SELECT has_setting_privilege('hash_mem_multiplier', 'set value');
SELECT has_setting_privilege((SELECT oid FROM pg_catalog.pg_setting_acl WHERE setting = 'work_mem'),
							 'alter system with grant option');
-- Check setting privileges show up in view
SELECT grantee, setting, privilege_type, is_grantable
	FROM pg_catalog.pg_setting_privileges
	ORDER BY grantee, setting, privilege_type;
-- Perform all operations as user 'regress_host_resource_admin' --
SET SESSION AUTHORIZATION regress_host_resource_admin;
ALTER SYSTEM SET autovacuum_work_mem = 32;  -- ok, privileges have been granted
ALTER SYSTEM SET ignore_system_indexes = OFF;  -- fail, insufficient privileges
ALTER SYSTEM RESET autovacuum_multixact_freeze_max_age;  -- fail, insufficient privileges
SET jit_provider = 'llvmjit';  -- fail, insufficient privileges
ALTER SYSTEM SET shared_buffers = 50;  -- ok
ALTER SYSTEM RESET shared_buffers;  -- ok
SET autovacuum_work_mem = 50;  -- cannot be changed now
ALTER SYSTEM RESET temp_file_limit;  -- ok
SET TimeZone = 'Europe/Helsinki';  -- ok
ALTER SYSTEM RESET autovacuum_work_mem;  -- ok, privileges have been granted
RESET TimeZone;  -- ok
SET SESSION AUTHORIZATION regress_admin;
DROP ROLE regress_host_resource_admin;  -- fail, privileges remain
-- Use "revoke" to remove the privileges and allow the role to be dropped
REVOKE SET VALUE, ALTER SYSTEM ON
	autovacuum_work_mem, hash_mem_multiplier, logical_decoding_work_mem,
	maintenance_work_mem, max_stack_depth, min_dynamic_shared_memory,
	shared_buffers, temp_buffers, temp_file_limit, work_mem
FROM regress_host_resource_admin;
DROP ROLE regress_host_resource_admin;  -- ok
-- Try that again, but use "drop owned by" instead of "revoke"
CREATE ROLE regress_host_resource_admin NOSUPERUSER;
SET SESSION AUTHORIZATION regress_host_resource_admin;
ALTER SYSTEM SET autovacuum_work_mem = 32;  -- fail, privileges not yet granted
SET SESSION AUTHORIZATION regress_admin;
GRANT SET VALUE, ALTER SYSTEM ON
	autovacuum_work_mem, hash_mem_multiplier, logical_decoding_work_mem,
	maintenance_work_mem, max_stack_depth, min_dynamic_shared_memory,
	shared_buffers, temp_buffers, temp_file_limit, work_mem
TO regress_host_resource_admin;
DROP ROLE regress_host_resource_admin;  -- fail, privileges remain
DROP OWNED BY regress_host_resource_admin RESTRICT; -- cascade should not be needed
SET SESSION AUTHORIZATION regress_host_resource_admin;
ALTER SYSTEM SET autovacuum_work_mem = 32;  -- fail, "drop owned" has dropped privileges
SET SESSION AUTHORIZATION regress_admin;
DROP ROLE regress_host_resource_admin;  -- ok
-- Try that again, but use "reassign owned by" this time
CREATE ROLE regress_host_resource_admin NOSUPERUSER;
CREATE ROLE regress_host_resource_newadmin NOSUPERUSER;
GRANT SET VALUE, ALTER SYSTEM ON
	autovacuum_work_mem, hash_mem_multiplier, logical_decoding_work_mem,
	maintenance_work_mem, max_stack_depth, min_dynamic_shared_memory,
	shared_buffers, temp_buffers, temp_file_limit, work_mem
TO regress_host_resource_admin;
REASSIGN OWNED BY regress_host_resource_admin TO regress_host_resource_newadmin;
SET SESSION AUTHORIZATION regress_host_resource_admin;
ALTER SYSTEM SET autovacuum_work_mem = 32;  -- ok, "reassign owned" did not change privileges
SET SESSION AUTHORIZATION regress_admin;
DROP ROLE regress_host_resource_admin;  -- fail, privileges remain
DROP ROLE regress_host_resource_newadmin;  -- ok, nothing was transferred
-- Use "drop owned by" so we can drop the role
DROP OWNED BY regress_host_resource_admin;  -- ok
DROP ROLE regress_host_resource_admin;  -- ok
-- Create non-superuser with privileges to configure plgsql custom variables
CREATE ROLE regress_plpgsql_admin NOSUPERUSER;
-- Perform all operations as user 'regress_plpgsql_admin' --
SET SESSION AUTHORIZATION regress_plpgsql_admin;
SET plpgsql.extra_errors TO 'all';
SET plpgsql.extra_warnings TO 'all';
RESET plpgsql.extra_errors;
RESET plpgsql.extra_warnings;
ALTER SYSTEM SET plpgsql.extra_errors TO 'all';
ALTER SYSTEM SET plpgsql.extra_warnings TO 'all';
ALTER SYSTEM RESET plpgsql.extra_errors;
ALTER SYSTEM RESET plpgsql.extra_warnings;
SET SESSION AUTHORIZATION regress_admin;
GRANT SET VALUE, ALTER SYSTEM ON
	plpgsql.extra_warnings, plpgsql.extra_errors
TO regress_plpgsql_admin;
-- Perform all operations as user 'regress_plpgsql_admin' --
SET SESSION AUTHORIZATION regress_plpgsql_admin;  -- ok
SET plpgsql.extra_errors TO 'all';  -- ok
SET plpgsql.extra_warnings TO 'all';  -- ok
RESET plpgsql.extra_errors;  -- ok
RESET plpgsql.extra_warnings;  -- ok
ALTER SYSTEM SET plpgsql.extra_errors TO 'all';  -- ok
ALTER SYSTEM SET plpgsql.extra_warnings TO 'all';  -- ok
ALTER SYSTEM RESET plpgsql.extra_errors;  -- ok
ALTER SYSTEM RESET plpgsql.extra_warnings;  -- ok
SET SESSION AUTHORIZATION regress_admin;
DROP ROLE regress_plpgsql_admin;  -- fail, privileges remain
REVOKE SET VALUE, ALTER SYSTEM ON
	plpgsql.extra_warnings, plpgsql.extra_errors
FROM regress_plpgsql_admin;
DROP ROLE regress_plpgsql_admin;  -- ok
