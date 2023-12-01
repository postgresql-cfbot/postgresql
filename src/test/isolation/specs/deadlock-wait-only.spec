setup
{
  CREATE TABLE a1 ();
}

teardown
{
  DROP TABLE a1;
}

session s1
setup		{ BEGIN; }
step s1re	  { LOCK TABLE a1 IN ROW EXCLUSIVE MODE; }
step s1aewo	{ LOCK TABLE a1 IN ACCESS EXCLUSIVE MODE WAIT ONLY; }
step s1c	  { COMMIT; }

session s2
setup		{ BEGIN; }
step s2as		{ LOCK TABLE a1 IN ACCESS SHARE MODE; }
step s2swo	{ LOCK TABLE a1 IN SHARE MODE WAIT ONLY; }
step s2c		{ COMMIT; }

permutation s1re s2as s2swo s1aewo s1c s2c
