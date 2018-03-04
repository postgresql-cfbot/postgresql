set enable_seqscan=off;

CREATE TABLE test_tsrange (
	i tsrange
);

INSERT INTO test_tsrange VALUES
	( 'empty' ),
	( '(,)' ),
	( '[2018-02-02 03:55:08,2018-04-02 03:55:08)' ),
	( '[2018-02-02 04:55:08,2018-04-02 04:55:08)' ),
	( '[2018-02-02 05:55:08,2018-04-02 05:55:08)' ),
	( '[2018-02-02 08:55:08,2018-04-02 08:55:08)' ),
	( '[2018-02-02 09:55:08,2018-04-02 09:55:08)' ),
	( '[2018-02-02 10:55:08,2018-04-02 10:55:08)' ),
	( '[infinity,infinity]' )
;

CREATE INDEX idx_tsrange ON test_tsrange USING gin (i);

SELECT * FROM test_tsrange WHERE i<'[2018-02-02 08:55:08,2018-04-02 08:55:08)'::tsrange ORDER BY i;
SELECT * FROM test_tsrange WHERE i<='[2018-02-02 08:55:08,2018-04-02 08:55:08)'::tsrange ORDER BY i;
SELECT * FROM test_tsrange WHERE i='[2018-02-02 08:55:08,2018-04-02 08:55:08)'::tsrange ORDER BY i;
SELECT * FROM test_tsrange WHERE i>='[2018-02-02 08:55:08,2018-04-02 08:55:08)'::tsrange ORDER BY i;
SELECT * FROM test_tsrange WHERE i>'[2018-02-02 08:55:08,2018-04-02 08:55:08)'::tsrange ORDER BY i;

EXPLAIN (COSTS OFF) SELECT * FROM test_tsrange WHERE i<'[2018-02-02 08:55:08,2018-04-02 08:55:08)'::tsrange ORDER BY i;
EXPLAIN (COSTS OFF) SELECT * FROM test_tsrange WHERE i<='[2018-02-02 08:55:08,2018-04-02 08:55:08)'::tsrange ORDER BY i;
EXPLAIN (COSTS OFF) SELECT * FROM test_tsrange WHERE i='[2018-02-02 08:55:08,2018-04-02 08:55:08)'::tsrange ORDER BY i;
EXPLAIN (COSTS OFF) SELECT * FROM test_tsrange WHERE i>='[2018-02-02 08:55:08,2018-04-02 08:55:08)'::tsrange ORDER BY i;
EXPLAIN (COSTS OFF) SELECT * FROM test_tsrange WHERE i>'[2018-02-02 08:55:08,2018-04-02 08:55:08)'::tsrange ORDER BY i;
