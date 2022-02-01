CREATE EXTENSION pg_walinspect;

SELECT pg_current_wal_lsn() AS wal_lsn \gset

CREATE TABLE sample_tbl(col1 int, col2 int);

INSERT INTO sample_tbl SELECT i, i+1 FROM generate_series(1, 100) i;

CHECKPOINT;

SELECT COUNT(*) >= 0 AS ok FROM pg_get_first_valid_wal_record_lsn(:'wal_lsn');

SELECT pg_get_first_valid_wal_record_lsn(:'wal_lsn') AS valid_wal_lsn \gset

SELECT COUNT(*) >= 0 AS ok FROM pg_get_raw_wal_record(:'valid_wal_lsn');

SELECT record AS raw_wal_rec FROM pg_get_raw_wal_record(:'valid_wal_lsn') \gset

SELECT * FROM pg_verify_raw_wal_record(:'raw_wal_rec');

SELECT COUNT(*) >= 0 AS ok FROM pg_get_wal_record_info(:'valid_wal_lsn');

INSERT INTO sample_tbl SELECT i, i+1 FROM generate_series(1, 100) i;

CHECKPOINT;

SELECT pg_current_wal_lsn() AS wal_lsn2 \gset

SELECT pg_get_first_valid_wal_record_lsn(:'wal_lsn2') AS valid_wal_lsn2 \gset

SELECT COUNT(*) >= 0 AS ok FROM pg_get_wal_record_info_2(:'valid_wal_lsn', :'valid_wal_lsn2') \gset

SELECT COUNT(*) >= 0 AS ok FROM pg_get_wal_stats(:'valid_wal_lsn', :'valid_wal_lsn2') \gset
