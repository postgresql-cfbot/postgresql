SET log_statement TO ddl;

-- should to fail
CREATE VARIABLE x AS int;

-- should be ok
CREATE TEMPORARY VARIABLE x AS int;

-- should fail
CREATE TEMPORARY VARIABLE x AS int;

-- should fail
DROP VARIABLE y;

-- should be ok
DROP VARIABLE x;

CREATE TYPE test_type AS (x int, y int);

-- should fail
CREATE VARIABLE x AS test_type;

DROP TYPE test_type;

-- should fail
CREATE VARIABLE x AS int[];

CREATE DOMAIN test_domain AS int;

-- should fail
CREATE TEMP VARIABLE x AS test_domain;

DROP DOMAIN test_domain;

CREATE ROLE regress_session_variable_test_role_01;
CREATE ROLE regress_session_variable_test_role_02;

SET ROLE TO regress_session_variable_test_role_01;

CREATE TEMP VARIABLE x AS int;

SET ROLE TO default;
SET ROLE TO regress_session_variable_test_role_02;

-- should fail
DROP VARIABLE x;

SET ROLE TO default;
SET ROLE TO regress_session_variable_test_role_01;

-- should be ok
DROP VARIABLE x;

SET ROLE TO DEFAULT;
DROP ROLE regress_session_variable_test_role_01;
DROP ROLE regress_session_variable_test_role_02;

CREATE TEMP VARIABLE x AS int;

-- should fail
CREATE TEMP VARIABLE x AS int;

DISCARD TEMP;

-- should be ok
CREATE TEMP VARIABLE x AS int;

-- should be ok
CREATE TEMP VARIABLE IF NOT EXISTS x AS int;

DROP VARIABLE x;
DROP VARIABLE IF EXISTS x;
