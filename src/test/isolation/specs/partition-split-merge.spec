# Verify that SPLIT and MERGE operations locks DML operations
# with partitioned table

setup
{
  DROP TABLE IF EXISTS tpart;
  CREATE TABLE tpart(i int, t text) partition by range(i);
  CREATE TABLE tpart_00_10 PARTITION OF tpart FOR VALUES FROM (0) TO (10);
  CREATE TABLE tpart_10_20 PARTITION OF tpart FOR VALUES FROM (10) TO (20);
  CREATE TABLE tpart_20_30 PARTITION OF tpart FOR VALUES FROM (20) TO (30);
  CREATE TABLE tpart_default PARTITION OF tpart DEFAULT;
}

teardown
{
  DROP TABLE tpart;
}

session s1
step s1b	{ BEGIN; }
step s1splt	{ ALTER TABLE tpart SPLIT PARTITION tpart_10_20 INTO
			   (PARTITION tpart_10_15 FOR VALUES FROM (10) TO (15),
			    PARTITION tpart_15_20 FOR VALUES FROM (15) TO (20)); }
step s1merg	{ ALTER TABLE tpart MERGE PARTITIONS (tpart_00_10, tpart_10_20) INTO tpart_00_20; }
step s1c	{ COMMIT; }


session s2
step s2b	{ BEGIN; }
step s2i	{ INSERT INTO tpart VALUES (1, 'text1'); }
step s2u	{ UPDATE tpart SET t = 'text1modif' where i = 1; }
step s2c	{ COMMIT; }
step s2s	{ SELECT * FROM tpart; }


# s1 starts SPLIT PARTITION then s2 trying to insert row and
# waits until s1 finished SPLIT operation.
permutation s1b s1splt s2b s2i s1c s2c s2s

# s2 inserts row into table. s1 starts MERGE PARTITIONS then
# s2 trying to update inserted row and waits until s1 finished
# MERGE operation.
permutation s2b s2i s2c s1b s1merg s2b s2u s1c s2c s2s
