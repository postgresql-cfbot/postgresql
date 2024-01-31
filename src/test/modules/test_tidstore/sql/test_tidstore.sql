CREATE EXTENSION test_tidstore;

-- Constant values used in the tests.
\set maxblkno 4294967295
-- The maximum number of heap tuples (MaxHeapTuplesPerPage) in 8kB block is 291.
-- We use a higher number to test tidstore.
\set maxoffset 512

-- Support functions.
CREATE FUNCTION make_tid(a bigint, b int2) RETURNS tid
BEGIN ATOMIC
RETURN ('(' || a || ', ' || b || ')')::tid;
END;

-- Lookup test function. Search 1 to (:maxoffset + 5) offset numbers in
-- 4 blocks, and return TIDS if found in the tidstore.
CREATE FUNCTION lookup_test() RETURNS SETOF tid
BEGIN ATOMIC;
WITH blocks (blk) AS (
VALUES (0), (2), (:maxblkno - 2), (:maxblkno)
)
SELECT t_ctid
  FROM
    (SELECT array_agg(make_tid(blk, off::int2)) AS tids
      FROM blocks, generate_series(1, :maxoffset + 5) off) AS foo,
    LATERAL tidstore_lookup_tids(foo.tids)
  WHERE found ORDER BY t_ctid;
END;

-- Test a local tdistore. A shared tidstore is created by passing true.
SELECT tidstore_create(false);

-- Test on empty tidstore.
SELECT *
    FROM tidstore_lookup_tids(ARRAY[make_tid(0, 1::int2),
        make_tid(:maxblkno, :maxoffset::int2)]::tid[]);
SELECT tidstore_is_full();

-- Add tids in random order.
WITH blocks (blk) AS(
VALUES (0), (1), (:maxblkno - 1), (:maxblkno / 2), (:maxblkno)
),
offsets (off) AS (
VALUES (1), (2), (:maxoffset / 2), (:maxoffset - 1), (:maxoffset)
)
SELECT row_number() over(ORDER BY blk),
     tidstore_set_block_offsets(blk, array_agg(offsets.off)::int2[])
  FROM blocks, offsets
  GROUP BY blk;

-- Lookup test and dump (sorted) tids.
SELECT lookup_test();
SELECT tidstore_is_full();
SELECT tidstore_dump_tids();

-- cleanup
SELECT tidstore_destroy();
DROP FUNCTION lookup_test();
DROP FUNCTION make_tid(a bigint, b int2);
