# Copyright (c) 2021-2023, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

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
	$lsn =
	  $node->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn();');

	my $logstart = -s $node->logfile;

	# Generate minimal WAL so that WAL buffers don't get overwritten.
	$node->safe_psql('postgres', "INSERT INTO t VALUES ($i);");

	$to_read = 8192;

	my $res = $node->safe_psql('postgres',
		qq{SELECT read_wal_from_buffers(lsn := '$lsn', bytes_to_read := $to_read) > 0;}
	);

	my $log = $node->log_contains(
		"request to flush past end of generated WAL; request .*, current position .*",
		$logstart);

	if ($res eq 't' && $log > 0)
	{
		$result = 1;
		last;
	}

	usleep(100_000);
}
ok($result, 'waited until WAL is successfully read from WAL buffers');

$result = 0;

# Wait until we get info of WAL records available in WAL buffers.
for (my $i = 0; $i < 10 * $PostgreSQL::Test::Utils::timeout_default; $i++)
{
	$node->safe_psql('postgres', "DROP TABLE IF EXISTS foo, bar;");
	$node->safe_psql('postgres',
		"CREATE TABLE foo AS SELECT * FROM generate_series(1, 2);");
	my $start_lsn =
	  $node->safe_psql('postgres', "SELECT pg_current_wal_insert_lsn();");
	my $tbl_oid = $node->safe_psql('postgres',
		"SELECT oid FROM pg_class WHERE relname = 'foo';");
	$node->safe_psql('postgres',
		"INSERT INTO foo SELECT * FROM generate_series(1, 10);");
	my $end_lsn =
	  $node->safe_psql('postgres', "SELECT pg_current_wal_insert_lsn();");
	$node->safe_psql('postgres',
		"CREATE TABLE bar AS SELECT * FROM generate_series(1, 2);");

	my $res = $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM get_wal_records_info_from_buffers('$start_lsn', '$end_lsn')
					WHERE block_ref LIKE concat('%', '$tbl_oid', '%') AND
						resource_manager = 'Heap' AND
						record_type = 'INSERT';");

	if ($res eq 10)
	{
		$result = 1;
		last;
	}

	usleep(100_000);
}
ok($result,
	'waited until we get info of WAL records available in WAL buffers.');

done_testing();
