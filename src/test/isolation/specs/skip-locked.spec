# Test SKIP LOCKED when regular row locks can't be acquired.

setup
{
  CREATE TABLE queue (
	id	int		PRIMARY KEY,
	data			text	NOT NULL,
	status			text	NOT NULL
  );
  INSERT INTO queue VALUES (1, 'foo', 'NEW'), (2, 'bar', 'NEW');
}

teardown
{
  DROP TABLE queue;
}

session s1
setup		{ BEGIN; }
step s1dc	{ DECLARE curs CURSOR FOR SELECT * FROM queue LIMIT 1 FOR UPDATE SKIP LOCKED; }
step s1f	{ FETCH curs; }
step s1u	{ UPDATE queue SET status = status + 1 WHERE CURRENT OF curs; }
step s1c	{ COMMIT; }

session s2
setup		{ BEGIN; }
step s2dc	{ DECLARE curs CURSOR FOR SELECT * FROM queue LIMIT 1 FOR UPDATE SKIP LOCKED; }
step s2f	{ FETCH curs; }
step s2u	{ UPDATE queue SET status = status + 1 WHERE CURRENT OF curs; }
step s2c	{ COMMIT; }
