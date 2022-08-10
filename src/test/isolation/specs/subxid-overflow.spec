# Subtransaction overflow
#
# This test is designed to cover some code paths which only occur when
# one transaction has overflowed the subtransaction cache.

setup
{
DROP TABLE IF EXISTS subxids;
CREATE TABLE subxids (subx integer);

CREATE OR REPLACE FUNCTION gen_subxids (n integer)
 RETURNS VOID
 LANGUAGE plpgsql
AS $$
BEGIN
  IF n <= 0 THEN
    INSERT INTO subxids VALUES (0);
    RETURN;
  ELSE
    PERFORM gen_subxids(n - 1);
    RETURN;
  END IF;
EXCEPTION /* generates a subxid */
  WHEN raise_exception THEN NULL;
END;
$$;
}

teardown
{
 DROP TABLE subxids;
 DROP FUNCTION gen_subxids(integer);
}

session s1
step subxid	{ BEGIN; SELECT gen_subxids(100); }
step s1c	{ COMMIT; }

session s2
# move xmax forwards
step xmax	{ BEGIN; INSERT INTO subxids VALUES (1); COMMIT;}
# will see step subxid as still running
step s2sel	{ SELECT count(*) FROM subxids WHERE subx = 0; }

# s2sel will see subxid as still running
permutation subxid xmax s2sel s1c
