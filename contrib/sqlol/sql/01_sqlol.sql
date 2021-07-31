LOAD 'sqlol';

-- create a base table, falling back on core grammar
CREATE TABLE t1 (id integer, val text);

-- test a SQLOL statement
HAI 1.2 I HAS A t1 GIMMEH id, "val" KTHXBYE\g

-- create a view in SQLOL
HAI 1.2 MAEK I HAS A t1 GIMMEH id, "val" A v0 KTHXBYE\g

-- combine standard SQL with a trailing SQLOL statement in multi-statements command
CREATE VIEW v1 AS SELECT * FROM t1\; CREATE VIEW v2 AS SELECT * FROM t1\;HAI 1.2 I HAS A t1 GIMMEH "id", id KTHXBYE\g

-- interleave standard SQL and SQLOL commands in multi-statements command
CREATE VIEW v3 AS SELECT * FROM t1\; HAI 1.2 MAEK I HAS A t1 GIMMEH id, "val" A v4 KTHXBYE CREATE VIEW v5 AS SELECT * FROM t1\;HAI 1.2 I HAS A t1 GIMMEH "id", id KTHXBYE\g

-- test MODE_SINGLE_QUERY with no trailing semicolon
SELECT 1\;SELECT 2\;SELECT 3 \g

-- test empty statement ignoring
\;\;select 1 \g

-- check the created views
SELECT relname, relkind
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE nspname = 'public'
ORDER BY relname COLLATE "C";

--
-- Error position
--
SELECT 1\;err;

-- sqlol won't trigger an error on incorrect GIMME keyword, so core parser will
-- complain about HAI
SELECT 1\;HAI 1.2 I HAS A t1 GIMME id KTHXBYE\g

-- sqlol will trigger the error about too many qualifiers on t1
SELECT 1\;HAI 1.2 I HAS A some.thing.public.t1 GIMMEH id KTHXBYE\g

-- position reported outside of the parser/scanner should be correct too
SELECT 1\;SELECT * FROM notatable;
