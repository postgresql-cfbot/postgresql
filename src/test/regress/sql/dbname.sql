CREATE ROLE dbuser1 with LOGIN;
CREATE ROLE dbuser2 with SUPERUSER LOGIN;
CREATE ROLE dbuser3 with SUPERUSER LOGIN;

CREATE DATABASE mydb1 with owner=dbuser1;
CREATE DATABASE "current_database" with owner=dbuser1;
CREATE DATABASE current_database with owner=dbuser1;

SELECT d.datname as "Name",
       pg_catalog.shobj_description(d.oid, 'pg_database') as "Description"
FROM pg_catalog.pg_database d
  	JOIN pg_catalog.pg_tablespace t on d.dattablespace = t.oid
ORDER BY 1;


\c mydb1;
SELECT CURRENT_DATABASE;


COMMENT ON DATABASE current_database IS 'db1';
COMMENT ON DATABASE "current_database" IS 'db2';

SELECT d.datname as "Name",
       pg_catalog.shobj_description(d.oid, 'pg_database') as "Description"
FROM pg_catalog.pg_database d
  	JOIN pg_catalog.pg_tablespace t on d.dattablespace = t.oid
ORDER BY 1;

-- test alter owner
ALTER DATABASE current_database OWNER to dbuser2; 
ALTER DATABASE "current_database" OWNER to dbuser2;

SELECT d.datname as "Name",
       pg_catalog.pg_get_userbyid(d.datdba) as "Owner",
       pg_catalog.shobj_description(d.oid, 'pg_database') as "Description"
FROM pg_catalog.pg_database d
  	JOIN pg_catalog.pg_tablespace t on d.dattablespace = t.oid
WHERE d.datname='current_database' or d.datname='mydb1'
ORDER BY 1;

-- test alter database tablespace
ALTER DATABASE current_database SET TABLESPACE pg_default;
ALTER DATABASE "current_database" SET TABLESPACE pg_default;

-- test alter database rename
ALTER DATABASE current_database rename to mydb2;
ALTER DATABASE "current_database" rename to mydb2;
ALTER DATABASE mydb2 rename to current_database;

SELECT d.datname as "Name",
       pg_catalog.shobj_description(d.oid, 'pg_database') as "Description"
FROM pg_catalog.pg_database d
  	JOIN pg_catalog.pg_tablespace t on d.dattablespace = t.oid
ORDER BY 1;

-- test alter database set parameter
ALTER DATABASE current_database SET parallel_tuple_cost=0.3;
\c mydb1
show parallel_tuple_cost;
ALTER DATABASE current_database RESET parallel_tuple_cost;
\c mydb1
show parallel_tuple_cost;

-- clean up
\c postgres

DROP DATABASE IF EXISTS "current_database";
DROP DATABASE IF EXISTS mydb1;
DROP DATABASE IF EXISTS mydb2;
DROP ROLE dbuser1;
DROP ROLE dbuser2;
DROP ROLE dbuser3;
