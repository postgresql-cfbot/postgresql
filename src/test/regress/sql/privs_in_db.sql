--
-- Tests for database-specific role memberships.
--

-- Clean up in case a prior regression run failed

-- Suppress NOTICE messages when users/groups don't exist
SET client_min_messages TO 'warning';

DROP ROLE IF EXISTS regress_priv_group1;
DROP ROLE IF EXISTS regress_priv_group2;

DROP DATABASE IF EXISTS regression_db_4;
DROP DATABASE IF EXISTS regression_db_3;
DROP DATABASE IF EXISTS regression_db_2;
DROP DATABASE IF EXISTS regression_db_1;
DROP ROLE IF EXISTS regress_role_granted;
DROP ROLE IF EXISTS regress_role_read_34;
DROP ROLE IF EXISTS regress_role_inherited_3;
DROP ROLE IF EXISTS regress_role_inherited_34;
DROP ROLE IF EXISTS regress_role_read_0;
DROP ROLE IF EXISTS regress_role_read_12;
DROP ROLE IF EXISTS regress_role_read_12_noinherit;
DROP ROLE IF EXISTS regress_role_read_all_noinherit;
DROP ROLE IF EXISTS regress_role_read_all_with_admin;
DROP DATABASE IF EXISTS regression_db_0;
DROP ROLE IF EXISTS regress_role_admin;

RESET client_min_messages;

-- test proper begins here

CREATE ROLE regress_role_admin LOGIN CREATEROLE CREATEDB;
GRANT pg_read_all_data TO regress_role_admin WITH ADMIN OPTION;
GRANT pg_read_all_stats TO regress_role_admin WITH ADMIN OPTION;
GRANT pg_maintain TO regress_role_admin WITH ADMIN OPTION;

-- Populate test databases.
CREATE DATABASE regression_db_0 OWNER regress_role_admin;

\connect regression_db_0
CREATE TABLE data AS SELECT generate_series(1, 3);

CREATE VIEW regress_role_memberships
 AS
SELECT
  r.rolname as role,
  m.rolname as member,
  CASE WHEN g.rolsuper THEN 'superuser' ELSE g.rolname END as grantor,
  admin_option,
  d.datname
FROM pg_auth_members a
JOIN pg_roles r ON r.oid = a.roleid
JOIN pg_roles m ON m.oid = a.member
LEFT JOIN pg_roles g ON g.oid = a.grantor
LEFT JOIN pg_database d ON d.oid = a.dbid
WHERE
  m.rolname LIKE 'regress_role_%'
ORDER BY
  1, 2, 5, 3
;

CREATE DATABASE regression_db_1 TEMPLATE regression_db_0 OWNER regress_role_admin;
CREATE DATABASE regression_db_2 TEMPLATE regression_db_1 OWNER regress_role_admin;
CREATE DATABASE regression_db_3 TEMPLATE regression_db_1 OWNER regress_role_admin;
CREATE DATABASE regression_db_4 TEMPLATE regression_db_1 OWNER regress_role_admin;

SET SESSION AUTHORIZATION regress_role_admin;

-- Read all cluster-wide with admin option
CREATE ROLE regress_role_read_all_with_admin ROLE regress_role_admin;
GRANT pg_read_all_data TO regress_role_read_all_with_admin WITH ADMIN OPTION;

-- Read all in databases 1 and 2
CREATE ROLE regress_role_read_12 ROLE regress_role_admin;
GRANT pg_read_all_data TO regress_role_read_12 IN DATABASE regression_db_1;
GRANT pg_read_all_data TO regress_role_read_12 IN DATABASE regression_db_2;

-- Read all in databases 3 and 4 with admin option
CREATE ROLE regress_role_read_34 ROLE regress_role_admin;
GRANT pg_read_all_data TO regress_role_read_34 IN DATABASE regression_db_3 WITH ADMIN OPTION;
GRANT pg_read_all_data TO regress_role_read_34 IN DATABASE regression_db_4 WITH ADMIN OPTION;

-- Inherits read all in databases 3 and 4
CREATE ROLE regress_role_inherited_34 ROLE regress_role_admin;
GRANT regress_role_read_34 TO regress_role_inherited_34;

-- Inherits read all in database 3
CREATE ROLE regress_role_inherited_3 ROLE regress_role_admin;
GRANT regress_role_read_34 TO regress_role_inherited_3 IN DATABASE regression_db_3;

-- No inherit
CREATE ROLE regress_role_read_all_noinherit NOINHERIT ROLE regress_role_admin;
GRANT regress_role_read_all_with_admin TO regress_role_read_all_noinherit;

-- No inherit in databases 1 and 2
CREATE ROLE regress_role_read_12_noinherit NOINHERIT ROLE regress_role_admin;
GRANT regress_role_read_12 TO regress_role_read_12_noinherit;

-- Alternate syntax
CREATE ROLE regress_role_read_0;
GRANT pg_read_all_data TO regress_role_read_0, regress_role_read_all_noinherit IN CURRENT DATABASE;

-- Failure due to missing database
GRANT pg_read_all_data TO regress_role_read_0 IN DATABASE non_existent; -- error

-- Should warn on duplicate grants
GRANT pg_read_all_data TO regress_role_read_all_with_admin; -- notice
GRANT pg_read_all_data TO regress_role_read_0 IN DATABASE regression_db_0; -- notice

-- Should not warn if adjusting admin option
GRANT pg_read_all_data TO regress_role_read_0 IN DATABASE regression_db_0 WITH ADMIN OPTION; -- silent
GRANT pg_read_all_data TO regress_role_read_0 IN DATABASE regression_db_0 WITH ADMIN OPTION; -- notice

GRANT pg_maintain TO regress_role_read_12 IN DATABASE regression_db_2;

-- Cluster-wide role
GRANT pg_read_all_stats TO regress_role_read_0;
GRANT pg_read_all_stats TO regress_role_read_34 IN DATABASE regression_db_3;  -- makes no sense XXX

-- Check membership table
TABLE regress_role_memberships;

-- Test membership privileges (regression_db_1)
\connect regression_db_1
SET SESSION AUTHORIZATION regress_role_admin;
SET ROLE regress_role_read_all_with_admin;
SELECT * FROM data; -- success
SET ROLE regress_role_read_12;
SELECT * FROM data; -- success
SET ROLE regress_role_read_34;
SELECT * FROM data; -- error
SET ROLE regress_role_inherited_34;
SELECT * FROM data; -- error
SET ROLE regress_role_inherited_3;
SELECT * FROM data; -- error
SET ROLE regress_role_read_all_noinherit;
SELECT * FROM data; -- error
SET ROLE regress_role_read_12_noinherit;
SELECT * FROM data; -- error

SET SESSION AUTHORIZATION regress_role_read_12;
VACUUM data; -- error
SET ROLE pg_read_all_data; -- success

SET SESSION AUTHORIZATION regress_role_inherited_34;
SET ROLE pg_read_all_data; -- error
SET ROLE regress_role_read_34; -- success

SET SESSION AUTHORIZATION regress_role_inherited_3;
SET ROLE pg_read_all_data; -- error
SET ROLE regress_role_read_34; -- error

SET SESSION AUTHORIZATION regress_role_read_all_noinherit;
SELECT * FROM data; -- error
SET ROLE pg_read_all_data; -- success
SELECT * FROM data; -- success

SET SESSION AUTHORIZATION regress_role_read_12_noinherit;
SELECT * FROM data; -- error
SET ROLE regress_role_read_12; -- success
SELECT * FROM data; -- success

-- Test membership privileges (regression_db_2)
\connect regression_db_2
SET SESSION AUTHORIZATION regress_role_admin;
SET ROLE regress_role_read_all_with_admin;
SELECT * FROM data; -- success
SET ROLE regress_role_read_12;
SELECT * FROM data; -- success
SET ROLE regress_role_read_34;
SELECT * FROM data; -- error
SET ROLE regress_role_inherited_34;
SELECT * FROM data; -- error
SET ROLE regress_role_inherited_3;
SELECT * FROM data; -- error
SET ROLE regress_role_read_all_noinherit;
SELECT * FROM data; -- error
SET ROLE regress_role_read_12_noinherit;
SELECT * FROM data; -- error

SET SESSION AUTHORIZATION regress_role_read_12;
VACUUM data; -- success
SET ROLE pg_read_all_data; -- success

SET SESSION AUTHORIZATION regress_role_inherited_34;
SET ROLE pg_read_all_data; -- error
SET ROLE regress_role_read_34; -- success

SET SESSION AUTHORIZATION regress_role_inherited_3;
SET ROLE pg_read_all_data; -- error
SET ROLE regress_role_read_34; -- error

SET SESSION AUTHORIZATION regress_role_read_all_noinherit;
SELECT * FROM data; -- error
SET ROLE pg_read_all_data; -- success
SELECT * FROM data; -- success

SET SESSION AUTHORIZATION regress_role_read_12_noinherit;
SELECT * FROM data; -- error
SET ROLE regress_role_read_12; -- success
SELECT * FROM data; -- success

-- Test membership privileges (regression_db_3)
\connect regression_db_3
SET SESSION AUTHORIZATION regress_role_admin;
SET ROLE regress_role_read_all_with_admin;
SELECT * FROM data; -- success
SET ROLE regress_role_read_12;
SELECT * FROM data; -- error
SET ROLE regress_role_read_34;
SELECT * FROM data; -- success
SET ROLE regress_role_inherited_34;
SELECT * FROM data; -- success
SET ROLE regress_role_inherited_3;
SELECT * FROM data; -- success
SET ROLE regress_role_read_all_noinherit;
SELECT * FROM data; -- error
SET ROLE regress_role_read_12_noinherit;
SELECT * FROM data; -- error

SET SESSION AUTHORIZATION regress_role_read_12;
SET ROLE pg_read_all_data; -- error

SET SESSION AUTHORIZATION regress_role_inherited_34;
SET ROLE pg_read_all_data; -- success
SET ROLE regress_role_read_34; -- success

SET SESSION AUTHORIZATION regress_role_inherited_3;
SET ROLE pg_read_all_data; -- success
SET ROLE regress_role_read_34; -- success

SET SESSION AUTHORIZATION regress_role_read_all_noinherit;
SELECT * FROM data; -- error
SET ROLE pg_read_all_data; -- success
SELECT * FROM data; -- success

SET SESSION AUTHORIZATION regress_role_read_12_noinherit;
SELECT * FROM data; -- error
SET ROLE regress_role_read_12; -- error
SELECT * FROM data; -- error

-- Test membership privileges (regression_db_4)
\connect regression_db_4
SET SESSION AUTHORIZATION regress_role_admin;
SET ROLE regress_role_read_all_with_admin;
SELECT * FROM data; -- success
SET ROLE regress_role_read_12;
SELECT * FROM data; -- error
SET ROLE regress_role_read_34;
SELECT * FROM data; -- success
SET ROLE regress_role_inherited_34;
SELECT * FROM data; -- success
SET ROLE regress_role_inherited_3;
SELECT * FROM data; -- error
SET ROLE regress_role_read_all_noinherit;
SELECT * FROM data; -- error
SET ROLE regress_role_read_12_noinherit;
SELECT * FROM data; -- error

SET SESSION AUTHORIZATION regress_role_read_12;
SET ROLE pg_read_all_data; -- error

SET SESSION AUTHORIZATION regress_role_inherited_34;
SET ROLE pg_read_all_data; -- success
SET ROLE regress_role_read_34; -- success

SET SESSION AUTHORIZATION regress_role_inherited_3;
SET ROLE pg_read_all_data; -- error
SET ROLE regress_role_read_34; -- error

SET SESSION AUTHORIZATION regress_role_read_all_noinherit;
SELECT * FROM data; -- error
SET ROLE pg_read_all_data; -- success
SELECT * FROM data; -- success

SET SESSION AUTHORIZATION regress_role_read_12_noinherit;
SELECT * FROM data; -- error
SET ROLE regress_role_read_12; -- error
SELECT * FROM data; -- error

\connect regression_db_0

-- Test cluster-wide role
SET SESSION AUTHORIZATION regress_role_read_0;
SELECT query FROM pg_stat_activity WHERE datname = 'regression_db_0';
SET SESSION AUTHORIZATION regress_role_read_12;
SELECT query FROM pg_stat_activity WHERE datname = 'regression_db_0';

\connect regression_db_3
SET SESSION AUTHORIZATION regress_role_read_34;
SELECT application_name, query FROM pg_stat_activity WHERE datname = 'regression_db_3';

\connect regression_db_0
SET SESSION AUTHORIZATION regress_role_admin;

-- Should not warn if revoking admin option
REVOKE ADMIN OPTION FOR pg_read_all_data FROM regress_role_read_0 IN DATABASE regression_db_0; -- silent
REVOKE ADMIN OPTION FOR pg_read_all_data FROM regress_role_read_0 IN DATABASE regression_db_0; -- silent
TABLE regress_role_memberships;

-- Should warn if revoking a non-existent membership
REVOKE pg_read_all_data FROM regress_role_read_0 IN DATABASE regression_db_0; -- success
REVOKE pg_read_all_data FROM regress_role_read_0 IN DATABASE regression_db_0; -- warning
TABLE regress_role_memberships;

-- Revoke should only apply to the specified level
REVOKE pg_read_all_data FROM regress_role_read_12; -- warning
TABLE regress_role_memberships;

-- Ensure cluster-wide admin option can grant cluster-wide and in specific databases
CREATE ROLE regress_role_granted;
SET SESSION AUTHORIZATION regress_role_read_all_with_admin;
GRANT pg_read_all_data TO regress_role_granted; -- success
GRANT pg_read_all_data TO regress_role_granted IN CURRENT DATABASE; -- success
GRANT pg_read_all_data TO regress_role_granted IN DATABASE regression_db_1; -- success
GRANT regress_role_read_34 TO regress_role_granted; -- error
TABLE regress_role_memberships;

-- Ensure database-specific admin option can only grant within that database
SET SESSION AUTHORIZATION regress_role_read_34;
GRANT pg_read_all_data TO regress_role_granted; -- error
GRANT pg_read_all_data TO regress_role_granted IN CURRENT DATABASE; -- error
GRANT pg_read_all_data TO regress_role_granted IN DATABASE regression_db_3; -- error
GRANT pg_read_all_data TO regress_role_granted IN DATABASE regression_db_4; -- error

\connect regression_db_3
SET SESSION AUTHORIZATION regress_role_read_34;
GRANT pg_read_all_data TO regress_role_granted; -- error
GRANT pg_read_all_data TO regress_role_granted IN CURRENT DATABASE; -- success
GRANT pg_read_all_data TO regress_role_granted IN DATABASE regression_db_3; -- notice
GRANT pg_read_all_data TO regress_role_granted IN DATABASE regression_db_4; -- error

\connect regression_db_4
SET SESSION AUTHORIZATION regress_role_read_34;
GRANT pg_read_all_data TO regress_role_granted; -- error
GRANT pg_read_all_data TO regress_role_granted IN CURRENT DATABASE; -- success
GRANT pg_read_all_data TO regress_role_granted IN DATABASE regression_db_3; -- error
GRANT pg_read_all_data TO regress_role_granted IN DATABASE regression_db_4; -- notice

\connect regression_db_0
SET SESSION AUTHORIZATION regress_role_admin;
TABLE regress_role_memberships;

-- Should clean up the membership table when dropping a database
DROP DATABASE regression_db_4;
DROP DATABASE regression_db_3;
DROP DATABASE regression_db_2;
DROP DATABASE regression_db_1;
TABLE regress_role_memberships;

-- Should clean up the membership table when dropping a role
DROP ROLE regress_role_granted;  -- dependency of 'regress_role_read_34'
DROP ROLE regress_role_read_34;
DROP ROLE regress_role_inherited_3;
DROP ROLE regress_role_inherited_34;
DROP ROLE regress_role_read_0;
DROP ROLE regress_role_read_12;
DROP ROLE regress_role_read_12_noinherit;
DROP ROLE regress_role_read_all_noinherit;
DROP ROLE regress_role_read_all_with_admin;

RESET SESSION AUTHORIZATION;
DROP OWNED BY regress_role_admin CASCADE;
TABLE regress_role_memberships;
\connect template1
DROP DATABASE regression_db_0;
DROP ROLE regress_role_admin;
SELECT datname FROM pg_database WHERE datname LIKE 'regression_db_%';
SELECT rolname FROM pg_roles WHERE rolname LIKE 'regress_role_%';
