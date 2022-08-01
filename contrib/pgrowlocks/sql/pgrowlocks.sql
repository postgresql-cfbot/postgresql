--
-- Test pgrowlocks extension
--

CREATE EXTENSION pgrowlocks;

-- set up
CREATE TABLE test(c int);

-- check permission to use pgrowlocks extension;

CREATE ROLE regress_priv_user1 superuser login;
CREATE ROLE regress_priv_user2;
CREATE ROLE regress_priv_user3;

SET SESSION AUTHORIZATION regress_priv_user1;

SELECT locked_row, locker, multi, xids, modes FROM pgrowlocks('test'); -- ok

SET SESSION AUTHORIZATION regress_priv_user2;
SELECT locked_row, locker, multi, xids, modes FROM pgrowlocks('test'); -- fail

-- switch to superuser
\c -

GRANT SELECT ON test TO regress_priv_user3;

SET SESSION AUTHORIZATION regress_priv_user3;
SELECT locked_row, locker, multi, xids, modes FROM pgrowlocks('test'); -- ok

