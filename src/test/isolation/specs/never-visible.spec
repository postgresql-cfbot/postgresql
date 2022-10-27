# Never visible test
#
# Test pruning of rows that were never visible outside of the
# originating transaction.

setup
{
 CREATE TABLE t (id int NOT NULL, txt text) WITH (fillfactor=50);
}

teardown
{
 DROP TABLE t;
}

session s1
step s1b	{ BEGIN; }
step s1i	{ INSERT INTO t (id) VALUES (1); }
step s1c	{ COMMIT; }

session s2
step s2b	{ BEGIN; }
step s2i	{ INSERT INTO t (id) VALUES (2); }
step s2u	{ UPDATE t SET txt = 'a' WHERE id = 2; }
step s2c	{ COMMIT; }
step s2v	{ VACUUM t; }

permutation s1b s1i s2b s2i s2u s2u s2c s2v s1c
