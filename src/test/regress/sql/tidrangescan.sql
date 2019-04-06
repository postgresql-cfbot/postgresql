-- tests for tidrangescans

CREATE TABLE tidrangescan(id integer, data text);

INSERT INTO tidrangescan SELECT i,repeat('x', 100) FROM generate_series(1,1000) AS s(i);
DELETE FROM tidrangescan WHERE substring(ctid::text from ',(\d+)\)')::integer > 10 OR substring(ctid::text from '\((\d+),')::integer >= 10;;
VACUUM tidrangescan;

-- range scans with upper bound
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid < '(1,0)';
SELECT ctid FROM tidrangescan WHERE ctid < '(1,0)';

EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid <= '(1,5)';
SELECT ctid FROM tidrangescan WHERE ctid <= '(1,5)';

EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid < '(0,0)';
SELECT ctid FROM tidrangescan WHERE ctid < '(0,0)';

-- range scans with lower bound
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid > '(9,8)';
SELECT ctid FROM tidrangescan WHERE ctid > '(9,8)';

EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE '(9,8)' < ctid;
SELECT ctid FROM tidrangescan WHERE '(9,8)' < ctid;

EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid >= '(9,8)';
SELECT ctid FROM tidrangescan WHERE ctid >= '(9,8)';

EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid >= '(100,0)';
SELECT ctid FROM tidrangescan WHERE ctid >= '(100,0)';

-- range scans with both bounds
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid > '(4,4)' AND '(4,7)' >= ctid;
SELECT ctid FROM tidrangescan WHERE ctid > '(4,4)' AND '(4,7)' >= ctid;

EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE '(4,7)' >= ctid AND ctid > '(4,4)';
SELECT ctid FROM tidrangescan WHERE '(4,7)' >= ctid AND ctid > '(4,4)';

-- extreme offsets
SELECT ctid FROM tidrangescan where ctid > '(0,65535)' AND ctid < '(1,0)' LIMIT 1;
SELECT ctid FROM tidrangescan where ctid < '(0,0)' LIMIT 1;

-- empty table
CREATE TABLE tidrangescan_empty(id integer, data text);

EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan_empty WHERE ctid < '(1, 0)';
SELECT ctid FROM tidrangescan_empty WHERE ctid < '(1, 0)';

EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan_empty WHERE ctid > '(9, 0)';
SELECT ctid FROM tidrangescan_empty WHERE ctid > '(9, 0)';

-- cursors
BEGIN;
DECLARE c CURSOR FOR SELECT ctid FROM tidrangescan WHERE ctid < '(1,0)';
FETCH NEXT c;
FETCH NEXT c;
FETCH PRIOR c;
FETCH FIRST c;
FETCH LAST c;
COMMIT;

DROP TABLE tidrangescan;
DROP TABLE tidrangescan_empty;
