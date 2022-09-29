
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
    "track_commit_timestamp = on\n"
    . "archive_mode=on\n"
    . "archive_command='/bin/false'\n"
    . "wal_level=logical");

# test pg_waldump with xlog files.
my $source_ts_path = PostgreSQL::Test::Utils::tempdir_short();
my $pg_wal_dir = $node->data_dir . "/pg_wal/";
my ($stdout, $stderr);

# test pg_waldump with hash index
$node->start;
$node->safe_psql('postgres',
    'BEGIN;'
    . 'CREATE TABLE hash_idx_tbl (
           id int
       );'
    . 'CREATE INDEX ON hash_idx_tbl USING hash (id);'
    . 'COMMIT;'
    . 'BEGIN;'
    . 'INSERT INTO hash_idx_tbl
       SELECT generate_series(1, 1000);'
    . 'COMMIT;'
    . 'select pg_switch_wal();'
);
$node->stop;

# track_commit_timestamp off
$node->append_conf('postgresql.conf',
    "track_commit_timestamp = off");

# test pg_waldump with gin index
$node->start;
$node->safe_psql('postgres',
    'BEGIN;'
    . 'CREATE TABLE gin_idx_tbl (
        id bigserial PRIMARY KEY,
        data jsonb
    );'
    . 'CREATE INDEX ON gin_idx_tbl USING gin (data);'
    . 'COMMIT;'
    . 'BEGIN;'
    . "INSERT INTO gin_idx_tbl
       WITH random_json AS (
            SELECT json_object_agg(key, trunc(random() * 10)) as json_data
                FROM unnest(array['a', 'b', 'c']) as u(key))
              SELECT generate_series(1,500), json_data FROM random_json;"
    . 'COMMIT;'
    . 'select pg_switch_wal();'
);
$node->stop;

# track_commit_timestamp on
$node->append_conf('postgresql.conf',
    "track_commit_timestamp = on");

## test pg_waldump with gist index
$node->start;
$node->safe_psql('postgres',
    'BEGIN;'
    . 'CREATE TABLE gist_idx_tbl (
    p point
    );'
    . 'CREATE INDEX ON gist_idx_tbl USING gist (p);'
    . 'COMMIT;'
    . 'BEGIN;'
    . 'INSERT INTO gist_idx_tbl(p) values (point \'(1,1)\');'
    . 'INSERT INTO gist_idx_tbl(p) values (point \'(3,2)\');'
    . 'INSERT INTO gist_idx_tbl(p) values (point \'(6,3)\');'
    . 'INSERT INTO gist_idx_tbl(p) values (point \'(5,5)\');'
    . 'INSERT INTO gist_idx_tbl(p) values (point \'(7,8)\');'
    . 'INSERT INTO gist_idx_tbl(p) values (point \'(8,6)\');'
    . 'INSERT INTO gist_idx_tbl(p) values (point \'(9,19)\');'
    . 'COMMIT;'
    . 'select pg_switch_wal();'
);
$node->stop;


## test pg_waldump with spgist index
$node->start;
$node->safe_psql('postgres',
    'BEGIN;'
    . 'CREATE TABLE spgist_idx_tbl (
    p point
    );'
    . 'CREATE INDEX ON spgist_idx_tbl USING spgist (p);'
    . 'COMMIT;'
    . 'BEGIN;'
    . 'INSERT INTO spgist_idx_tbl(p) values (point \'(1,1)\');'
    . 'INSERT INTO spgist_idx_tbl(p) values (point \'(3,2)\');'
    . 'INSERT INTO spgist_idx_tbl(p) values (point \'(6,3)\');'
    . 'INSERT INTO spgist_idx_tbl(p) values (point \'(5,5)\');'
    . 'INSERT INTO spgist_idx_tbl(p) values (point \'(7,8)\');'
    . 'INSERT INTO spgist_idx_tbl(p) values (point \'(8,6)\');'
    . 'INSERT INTO spgist_idx_tbl(p) values (point \'(9,19)\');'
    . 'COMMIT;'
);
$node->stop;

# test pg_waldump with brin index
$node->start;
$node->safe_psql('postgres',
    'BEGIN;'
    . 'CREATE TABLE brin_idx_tbl (
        col1 int,
        col2 text,
        col3 text
    );'
    . 'COMMIT;'
    . 'BEGIN;'
    . 'CREATE INDEX brin_idx ON brin_idx_tbl USING brin (col1, col2, col3) WITH (autosummarize=on);'
    . 'COMMIT;'
    . 'BEGIN;'
    . "INSERT INTO brin_idx_tbl
       SELECT generate_series(1, 10000), 'dummy', 'dummy';"
    . 'COMMIT;'
    . 'BEGIN;'
    . "UPDATE brin_idx_tbl
       SET col2 = 'updated'
       WHERE col1 BETWEEN 1 AND 5000;"
    . 'COMMIT;'
    . "SELECT brin_summarize_range('brin_idx', 0);"
    . "SELECT brin_desummarize_range('brin_idx', 0);"
);
$node->stop;

# test pg_waldump with tablespace
$node->start;
$node->safe_psql('postgres',
    "CREATE TABLESPACE tbl_space LOCATION '$source_ts_path';"
    . 'CREATE TABLE table_space_tbl (
        col1 int
    ) TABLESPACE tbl_space;'
);
$node->stop;

# test pg_waldump with sequence
$node->start;
$node->safe_psql('postgres',
    "CREATE SEQUENCE sequence;"
);
$node->stop;

# test local message
$node->start;
$node->safe_psql('postgres',
    'BEGIN;'
    . "SELECT pg_logical_emit_message(true, 'text', 'text');"
    . 'COMMIT;'
);
$node->stop;

# test relmap
$node->start;
$node->safe_psql('postgres',
    "SELECT relfilenode FROM pg_class WHERE relname = 'pg_authid';"
    . 'VACUUM FULL pg_authid;'
);
$node->stop;

chdir $pg_wal_dir;
foreach my $file (glob '000*') {
    IPC::Run::run [ 'pg_waldump', "$pg_wal_dir/$file" ], '>', \$stdout, '2>', \$stderr;
    isnt($stdout, '');
}

done_testing();
