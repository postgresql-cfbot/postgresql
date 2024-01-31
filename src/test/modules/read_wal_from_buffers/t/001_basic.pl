# Copyright (c) 2021-2023, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Setup a new node.  The configuration chosen here minimizes the number
# of arbitrary records that could get generated in a cluster.  Enlarging
# checkpoint_timeout avoids noise with checkpoint activity.  wal_level
# set to "minimal" avoids random standby snapshot records.  Autovacuum
# could also trigger randomly, generating random WAL activity of its own.
# Enlarging wal_writer_delay and wal_writer_flush_after avoid background
# wal flush by walwriter.
my $node = PostgreSQL::Test::Cluster->new("node");
$node->init;
$node->append_conf(
	'postgresql.conf',
	q[wal_level = minimal
	  autovacuum = off
	  checkpoint_timeout = '30min'
	  wal_writer_delay = 10000ms
	  wal_writer_flush_after = 1GB
]);
$node->start;

# Setup.
$node->safe_psql('postgres', 'CREATE EXTENSION read_wal_from_buffers;');

$node->safe_psql('postgres', 'CREATE TABLE t (c int);');

my $result = 0;
my $lsn;
my $to_read;

# Wait until we read from WAL buffers
for (my $i = 0; $i < 10 * $PostgreSQL::Test::Utils::timeout_default; $i++)
{
	# Get current insert LSN. After this, we generate some WAL which is guranteed
	# to be in WAL buffers as there is no other WAL generating activity is
	# happening on the server. We then verify if we can read the WAL from WAL
	# buffers using this LSN.
	$lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn();');

	# Generate minimal WAL so that WAL buffers don't get overwritten.
	$node->safe_psql('postgres', "INSERT INTO t VALUES ($i);");

	$to_read = 8192;

	if ($node->safe_psql('postgres',
		qq{SELECT read_wal_from_buffers(lsn := '$lsn', bytes_to_read := $to_read) > 0;}))
	{
		$result = 1;
		last;
	}

	usleep(100_000);
}
ok($result, 'waited until WAL is successfully read from WAL buffers');

# Check with a WAL that doesn't yet exist i.e., 16MB starting from current
# flush LSN.
$lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()+16777216;');
$to_read = 8192;
$result = $node->safe_psql('postgres',
	qq{SELECT read_wal_from_buffers(lsn := '$lsn', bytes_to_read := $to_read) = 0;});
is($result, 't', "WAL that doesn't yet exist is not read from WAL buffers");

done_testing();
