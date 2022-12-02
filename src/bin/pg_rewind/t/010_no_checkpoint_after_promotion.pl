
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;

use RewindTest;

RewindTest::setup_cluster("remote");
RewindTest::start_primary();

# Create a test table and insert a row in primary.
primary_psql("CREATE TABLE tbl1 (d text)");
primary_psql("INSERT INTO tbl1 VALUES ('in primary')");

# This test table will be used to test truncation, i.e. the table
# is extended in the old primary after promotion
primary_psql("CREATE TABLE trunc_tbl (d text)");
primary_psql("INSERT INTO trunc_tbl VALUES ('in primary')");

# This test table will be used to test the "copy-tail" case, i.e. the
# table is truncated in the old primary after promotion
primary_psql("CREATE TABLE tail_tbl (id integer, d text)");
primary_psql("INSERT INTO tail_tbl VALUES (0, 'in primary')");

# This test table is dropped in the old primary after promotion.
primary_psql("CREATE TABLE drop_tbl (d text)");
primary_psql("INSERT INTO drop_tbl VALUES ('in primary')");

primary_psql("CHECKPOINT");

RewindTest::create_standby("remote");

# Insert additional data on primary that will be replicated to standby
primary_psql("INSERT INTO tbl1 values ('in primary, before promotion')");
primary_psql(
  "INSERT INTO trunc_tbl values ('in primary, before promotion')");
primary_psql(
  "INSERT INTO tail_tbl SELECT g, 'in primary, before promotion: ' || g FROM generate_series(1, 10000) g"
);

primary_psql('CHECKPOINT');

RewindTest::promote_standby('skip_checkpoint' => 1);

# Insert a row in the old primary. This causes the primary and standby
# to have "diverged", it's no longer possible to just apply the
# standy's logs over primary directory - you need to rewind.
primary_psql("INSERT INTO tbl1 VALUES ('in primary, after promotion')");

# Also insert a new row in the standby, which won't be present in the
# old primary.
standby_psql("INSERT INTO tbl1 VALUES ('in standby, after promotion')");

$node_primary->stop;

my $primary_pgdata = $node_primary->data_dir;
my $standby_connstr = $node_standby->connstr('postgres');
command_checks_all(
  [
    'pg_rewind',       '--dry-run',
    "--source-server", $standby_connstr,
    '--target-pgdata', $primary_pgdata,
    '--no-sync',       '--no-ensure-shutdown'
  ],
  0,
  [],
  [qr/pg_rewind: source's actual timeline ID \(2\) is newer than control file \(1\)/],
  'pg_rewind no checkpoint after promotion');

done_testing();
