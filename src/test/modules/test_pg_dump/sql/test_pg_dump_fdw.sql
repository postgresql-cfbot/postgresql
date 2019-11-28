CREATE EXTENSION test_pg_dump_fdw;

CREATE SERVER pg_dump_fdw FOREIGN DATA WRAPPER test_pg_dump_fdw;

CREATE FOREIGN TABLE test_pg_dump_fdw_t (a INTEGER, b INTEGER) SERVER pg_dump_fdw;

SELECT * FROM test_pg_dump_fdw_t;
