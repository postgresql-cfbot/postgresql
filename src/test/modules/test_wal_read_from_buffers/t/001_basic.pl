# Copyright (c) 2021-2023, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');

$node->init;

# Ensure nobody interferes with us so that the WAL in WAL buffers don't get
# overwritten while running tests.
$node->append_conf(
	'postgresql.conf', qq(
autovacuum = off
checkpoint_timeout = 1h
wal_writer_delay = 10000ms
wal_writer_flush_after = 1GB
));
$node->start;

# Setup.
$node->safe_psql('postgres', 'CREATE EXTENSION test_wal_read_from_buffers');

# Get current insert LSN. After this, we generate some WAL which is guranteed
# to be in WAL buffers as there is no other WAL generating activity is
# happening on the server. We then verify if we can read the WAL from WAL
# buffers using this LSN.
my $lsn =
  $node->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn();');

# Generate minimal WAL so that WAL buffers don't get overwritten.
$node->safe_psql('postgres',
	"CREATE TABLE t (c int); INSERT INTO t VALUES (1);");

# Check if WAL is successfully read from WAL buffers.
my $result = $node->safe_psql('postgres',
	qq{SELECT test_wal_read_from_buffers('$lsn')});
is($result, 't', "WAL is successfully read from WAL buffers");

done_testing();
