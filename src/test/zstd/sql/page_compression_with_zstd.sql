--
-- Page compression tests
--

--
-- create compressed table
--
CREATE TABLE tbl_pc(id int, c1 text) WITH(compresstype=zstd);
\d+ tbl_pc
DROP TABLE tbl_pc;

CREATE TABLE tbl_pc(id int, c1 text) WITH(compresstype=zstd, compress_chunk_size=512);
\d+ tbl_pc
DROP TABLE tbl_pc;

CREATE TABLE tbl_pc(id int, c1 text) WITH(compresstype=zstd, compress_chunk_size=1024);
\d+ tbl_pc
DROP TABLE tbl_pc;

CREATE TABLE tbl_pc(id int, c1 text) WITH(compresstype=zstd, compress_chunk_size=2048);
\d+ tbl_pc
DROP TABLE tbl_pc;

CREATE TABLE tbl_pc(id int, c1 text) WITH(compresstype=zstd, compresslevel=0, compress_chunk_size=4096, compress_prealloc_chunks=0);
\d+ tbl_pc
DROP TABLE tbl_pc;

CREATE TABLE tbl_pc(id int PRIMARY KEY, c1 text) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);
\d+ tbl_pc
DROP TABLE tbl_pc;

CREATE TABLE tbl_pc(id int PRIMARY KEY, c1 text) WITH(compresstype=zstd, compresslevel=-1, compress_chunk_size=1024, compress_prealloc_chunks=15);
\d+ tbl_pc
DROP TABLE tbl_pc;

CREATE TABLE tbl_pc WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=15)
  AS SELECT id, id::text c1 FROM generate_series(1,1000)id;
\d+ tbl_pc
DROP TABLE tbl_pc;

CREATE TABLE tbl_pc(id int PRIMARY KEY WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2), c1 text) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);
CREATE TABLE tbl_pc2(LIKE tbl_pc INCLUDING ALL);
\d+ tbl_pc
DROP TABLE tbl_pc;
DROP TABLE tbl_pc2;

CREATE TABLE tbl_pc(id int PRIMARY KEY, c1 text) WITH(compresstype=none);
\d+ tbl_pc
DROP TABLE tbl_pc;

-- invalid storage parameter
CREATE TABLE tbl_pc_error(id int, c1 text) WITH(compresstype=xyz); -- fail
CREATE TABLE tbl_pc_error(id int, c1 text) WITH(compresstype=zstd, compresslevel=xyz); -- fail
CREATE TABLE tbl_pc_error(id int, c1 text) WITH(compresstype=zstd, compress_chunk_size=1025); -- fail
CREATE TABLE tbl_pc_error(id int, c1 text) WITH(compresstype=zstd, compress_prealloc_chunks=16); -- fail


--
-- create compressed index
--
SET enable_seqscan = OFF;

CREATE TABLE tbl_pc(id int PRIMARY KEY WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2), c1 text) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);

CREATE INDEX tbl_pc_idx1 on tbl_pc(c1) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);

\d+ tbl_pc

INSERT INTO tbl_pc SELECT id, id::text FROM generate_series(1,1000)id;

-- call CHECKPOINT to flush shared buffer to compressed relation file 
CHECKPOINT;

-- run ANALYZE REINDEX VACUUM and CLUSTER on compressed table and index
ANALYZE tbl_pc;

SELECT count(*) FROM tbl_pc;
SELECT * FROM tbl_pc WHERE c1='100';
EXPLAIN(COSTS off) SELECT * FROM tbl_pc WHERE c1='100';

REINDEX INDEX tbl_pc_idx1;
CHECKPOINT;
SELECT * FROM tbl_pc WHERE c1='100';

REINDEX TABLE tbl_pc;
CHECKPOINT;
SELECT * FROM tbl_pc WHERE c1='100';

VACUUM tbl_pc;
CHECKPOINT;
SELECT count(*) FROM tbl_pc;
SELECT * FROM tbl_pc WHERE c1='100';

VACUUM FULL tbl_pc;
CHECKPOINT;
SELECT count(*) FROM tbl_pc;
SELECT * FROM tbl_pc WHERE c1='100';

CLUSTER tbl_pc USING tbl_pc_idx1;
CHECKPOINT;
SELECT count(*) FROM tbl_pc;
SELECT * FROM tbl_pc WHERE c1='100';

DROP INDEX tbl_pc_idx1;

-- check usage of compressed index with data
CREATE INDEX tbl_pc_idx1 on tbl_pc USING hash(c1) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);
CHECKPOINT;
SELECT * FROM tbl_pc WHERE c1='100';
EXPLAIN(COSTS off) SELECT * FROM tbl_pc WHERE c1='100';
DROP INDEX tbl_pc_idx1;

CREATE INDEX tbl_pc_idx1 on tbl_pc USING gin((ARRAY[id])) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);
CHECKPOINT;
SELECT * FROM tbl_pc WHERE ARRAY[id] @> ARRAY[100];
EXPLAIN(COSTS off) SELECT * FROM tbl_pc WHERE ARRAY[id] @> ARRAY[100];
DROP INDEX tbl_pc_idx1;

CREATE INDEX tbl_pc_idx1 on tbl_pc USING gist((point(id,id))) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);
CHECKPOINT;
SELECT * FROM tbl_pc ORDER BY point(id,id) <-> point(100,100) limit 1;
EXPLAIN(COSTS off) SELECT * FROM tbl_pc ORDER BY point(id,id) <-> point(100,100) limit 1;
DROP INDEX tbl_pc_idx1;

CREATE INDEX tbl_pc_idx1 on tbl_pc USING spgist(c1) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);
CHECKPOINT;
SELECT * FROM tbl_pc WHERE c1='100';
EXPLAIN(COSTS off) SELECT * FROM tbl_pc WHERE c1='100';
DROP INDEX tbl_pc_idx1;

-- brin index does not support compression
CREATE INDEX tbl_pc_idx1 on tbl_pc USING brin(c1) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2); -- fail

DROP TABLE tbl_pc;
RESET enable_seqscan;

--
-- alter table and index
--

-- ALTER TABLE
-- ALTER compresstype and compress_chunk_size currently is not supported
CREATE TABLE tbl_pc(id int, c1 text);
ALTER TABLE tbl_pc SET(compresstype=zstd); -- fail
DROP TABLE tbl_pc;

CREATE TABLE tbl_pc(id int PRIMARY KEY, c1 text) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);
ALTER TABLE tbl_pc SET(compresstype=none); -- fail
ALTER TABLE tbl_pc SET(compress_chunk_size=2048); -- fail
ALTER TABLE tbl_pc SET(compress_prealloc_chunks=16);  -- fail
ALTER TABLE tbl_pc SET(compresslevel=2, compress_prealloc_chunks=0); -- ok
\d+ tbl_pc
ALTER TABLE tbl_pc RESET(compresstype); -- fail
ALTER TABLE tbl_pc RESET(compress_chunk_size); -- fail
ALTER TABLE tbl_pc RESET(compresslevel); -- ok
ALTER TABLE tbl_pc RESET(compress_prealloc_chunks); -- ok
\d+ tbl_pc
CHECKPOINT;
SELECT count(*) FROM tbl_pc;

-- ALTER INDEX
-- ALTER compresstype and compress_chunk_size currently is not supported
CREATE INDEX tbl_pc_idx1 on tbl_pc USING btree(c1);
ALTER INDEX tbl_pc_idx1 SET(compresstype=zstd); -- fail
DROP INDEX tbl_pc_idx1;

CREATE INDEX tbl_pc_idx1 on tbl_pc USING btree(c1) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);
ALTER INDEX tbl_pc_idx1 SET(compresstype=none); -- fail
ALTER INDEX tbl_pc_idx1 SET(compress_chunk_size=2048); -- fail
ALTER INDEX tbl_pc_idx1 SET(compress_prealloc_chunks=16);  -- fail
ALTER INDEX tbl_pc_idx1 SET(compresslevel=2, compress_prealloc_chunks=0); -- ok
\d+ tbl_pc
ALTER INDEX tbl_pc_idx1 RESET(compresstype); -- fail
ALTER INDEX tbl_pc_idx1 RESET(compress_chunk_size); -- fail
ALTER INDEX tbl_pc_idx1 RESET(compresslevel); -- ok
ALTER INDEX tbl_pc_idx1 RESET(compress_prealloc_chunks); -- ok
\d+ tbl_pc
CHECKPOINT;
SELECT * FROM tbl_pc WHERE c1='100';
EXPLAIN(COSTS off) SELECT * FROM tbl_pc WHERE c1='100';

-- alter hash index
CREATE INDEX tbl_pc_idx_hash on tbl_pc USING hash(c1) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);

ALTER INDEX tbl_pc_idx_hash SET(compresstype=none); -- fail
ALTER INDEX tbl_pc_idx_hash SET(compress_chunk_size=2048); -- fail
ALTER INDEX tbl_pc_idx_hash SET(compress_prealloc_chunks=16);  -- fail
ALTER INDEX tbl_pc_idx_hash SET(compresslevel=2, compress_prealloc_chunks=0); -- ok
\d+ tbl_pc_idx_hash
ALTER INDEX tbl_pc_idx_hash RESET(compresstype); -- fail
ALTER INDEX tbl_pc_idx_hash RESET(compress_chunk_size); -- fail
ALTER INDEX tbl_pc_idx_hash RESET(compresslevel); -- ok
ALTER INDEX tbl_pc_idx_hash RESET(compress_prealloc_chunks); -- ok
\d+ tbl_pc_idx_hash

-- alter gin index
CREATE INDEX tbl_pc_idx_gin on tbl_pc USING gin((ARRAY[id])) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);

ALTER INDEX tbl_pc_idx_gin SET(compresstype=none); -- fail
ALTER INDEX tbl_pc_idx_gin SET(compress_chunk_size=2048); -- fail
ALTER INDEX tbl_pc_idx_gin SET(compress_prealloc_chunks=16);  -- fail
ALTER INDEX tbl_pc_idx_gin SET(compresslevel=2, compress_prealloc_chunks=0); -- ok
\d+ tbl_pc_idx_gin
ALTER INDEX tbl_pc_idx_gin RESET(compresstype); -- fail
ALTER INDEX tbl_pc_idx_gin RESET(compress_chunk_size); -- fail
ALTER INDEX tbl_pc_idx_gin RESET(compresslevel); -- ok
ALTER INDEX tbl_pc_idx_gin RESET(compress_prealloc_chunks); -- ok
\d+ tbl_pc_idx_gin

-- alter gist index
CREATE INDEX tbl_pc_idx_gist on tbl_pc USING gist((point(id,id))) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);

ALTER INDEX tbl_pc_idx_gist SET(compresstype=none); -- fail
ALTER INDEX tbl_pc_idx_gist SET(compress_chunk_size=2048); -- fail
ALTER INDEX tbl_pc_idx_gist SET(compress_prealloc_chunks=16);  -- fail
ALTER INDEX tbl_pc_idx_gist SET(compresslevel=2, compress_prealloc_chunks=0); -- ok
\d+ tbl_pc_idx_gist
ALTER INDEX tbl_pc_idx_gist RESET(compresstype); -- fail
ALTER INDEX tbl_pc_idx_gist RESET(compress_chunk_size); -- fail
ALTER INDEX tbl_pc_idx_gist RESET(compresslevel); -- ok
ALTER INDEX tbl_pc_idx_gist RESET(compress_prealloc_chunks); -- ok
\d+ tbl_pc_idx_gist

-- alter spgist index
CREATE INDEX tbl_pc_idx_spgist on tbl_pc USING spgist(c1) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);

ALTER INDEX tbl_pc_idx_spgist SET(compresstype=none); -- fail
ALTER INDEX tbl_pc_idx_spgist SET(compress_chunk_size=2048); -- fail
ALTER INDEX tbl_pc_idx_spgist SET(compress_prealloc_chunks=16);  -- fail
ALTER INDEX tbl_pc_idx_spgist SET(compresslevel=2, compress_prealloc_chunks=0); -- ok
\d+ tbl_pc_idx_spgist
ALTER INDEX tbl_pc_idx_spgist RESET(compresstype); -- fail
ALTER INDEX tbl_pc_idx_spgist RESET(compress_chunk_size); -- fail
ALTER INDEX tbl_pc_idx_spgist RESET(compresslevel); -- ok
ALTER INDEX tbl_pc_idx_spgist RESET(compress_prealloc_chunks); -- ok
\d+ tbl_pc_idx_spgist

-- alter brin index (do not support compression)
CREATE INDEX tbl_pc_idx_brin on tbl_pc USING brin(c1);
ALTER INDEX tbl_pc_idx_brin SET(compress_prealloc_chunks=3);  -- fail

DROP TABLE tbl_pc;

--
-- partitioned table and index
--

-- partition table does not support compression, but index of partition table and its child tables can use compression
CREATE TABLE tbl_pc_part (id int, c1 text) PARTITION BY RANGE (id) WITH(compresstype=zstd); -- fail

CREATE TABLE tbl_pc_part (id int, c1 text) PARTITION BY RANGE (id);
CREATE TABLE tbl_pc_part_1 PARTITION OF tbl_pc_part FOR VALUES FROM (1) TO (1001);
CREATE TABLE tbl_pc_part_2 PARTITION OF tbl_pc_part FOR VALUES FROM (1001) TO (2001) WITH(compresstype=zstd, compresslevel=1, compress_chunk_size=1024, compress_prealloc_chunks=2);

CREATE INDEX part_id_idx ON tbl_pc_part(id) WITH(compresstype=zstd, compresslevel=2, compress_chunk_size=1024, compress_prealloc_chunks=2);

CREATE TABLE tbl_pc_part_3 PARTITION OF tbl_pc_part FOR VALUES FROM (2001) TO (3001);

CREATE INDEX part3_id_idx1 ON tbl_pc_part_3(id) WITH(compresstype=zstd, compresslevel=2, compress_chunk_size=1024, compress_prealloc_chunks=2);

\d+ tbl_pc_part
\d+ part_id_idx
\d+ tbl_pc_part_1
\d+ tbl_pc_part_2
\d+ tbl_pc_part_3

INSERT INTO tbl_pc_part SELECT id, id::text FROM generate_series(1,3000)id;
CHECKPOINT;
SELECT count(*) FROM tbl_pc_part;
SELECT * FROM tbl_pc_part WHERE id=100;
SELECT * FROM tbl_pc_part WHERE id=1100;
SELECT * FROM tbl_pc_part WHERE id=2100;

ALTER TABLE tbl_pc_part SET(compresstype=zstd); -- fail
ALTER TABLE tbl_pc_part_1 SET(compresstype=zstd); -- fail

ALTER TABLE tbl_pc_part_2 SET(compresstype=none); -- fail
ALTER TABLE tbl_pc_part_2 SET(compress_chunk_size=2048); -- fail
ALTER TABLE tbl_pc_part_2 SET(compress_prealloc_chunks=16);  -- fail
ALTER TABLE tbl_pc_part_2 SET(compresslevel=2, compress_prealloc_chunks=0); -- ok
\d+ tbl_pc_part_2

ALTER INDEX part3_id_idx1 SET(compresstype=none); -- fail
ALTER INDEX part3_id_idx1 SET(compress_chunk_size=2048); -- fail
ALTER INDEX part3_id_idx1 SET(compress_prealloc_chunks=16);  -- fail
ALTER INDEX part3_id_idx1 SET(compresslevel=2, compress_prealloc_chunks=0); -- ok
\d+ part3_id_idx1

ALTER INDEX part_id_idx SET(compresstype=zstd); -- fail
ALTER INDEX part_id_idx SET(compress_chunk_size=2048); -- fail
ALTER INDEX part_id_idx SET(compress_prealloc_chunks=16);  -- fail
ALTER INDEX part_id_idx SET(compresslevel=2, compress_prealloc_chunks=0); -- fail
\d+ tbl_pc_part

INSERT INTO tbl_pc_part SELECT id, id::text FROM generate_series(1,3000)id;
CHECKPOINT;
SELECT count(*) FROM tbl_pc_part;
SELECT * FROM tbl_pc_part WHERE id=100;
SELECT * FROM tbl_pc_part WHERE id=1100;
SELECT * FROM tbl_pc_part WHERE id=2100;

DROP TABLE tbl_pc_part;


--
-- recycling space with vacuum (do not support)
--
CREATE TABLE tbl_pc(id int PRIMARY KEY, c1 text) WITH(compresstype=zstd, compress_chunk_size=2048, compress_prealloc_chunks=0);
CHECKPOINT;
SELECT pg_relation_size('tbl_pc') size_0 \gset

INSERT INTO tbl_pc SELECT id, id::text FROM generate_series(1,1000)id;
CHECKPOINT;
SELECT pg_relation_size('tbl_pc') size_1000 \gset

SELECT :size_1000 > :size_0; -- true

DELETE FROM tbl_pc WHERE id > 500;
VACUUM tbl_pc;
SELECT pg_relation_size('tbl_pc') size_500 \gset
SELECT count(*) FROM tbl_pc;
SELECT :size_500 = :size_1000;  -- true

DELETE FROM tbl_pc WHERE id < 500;
VACUUM tbl_pc;
SELECT pg_relation_size('tbl_pc') size_1 \gset
SELECT count(*) FROM tbl_pc;
SELECT :size_1 = :size_500;  -- true

DELETE FROM tbl_pc;
VACUUM tbl_pc;
SELECT pg_relation_size('tbl_pc') size_0_2 \gset

SELECT :size_0_2 = :size_500;  -- true

VACUUM FULL tbl_pc;
SELECT pg_relation_size('tbl_pc') size_0_3 \gset

SELECT :size_0_3 = :size_0;  -- true

DROP TABLE tbl_pc;
