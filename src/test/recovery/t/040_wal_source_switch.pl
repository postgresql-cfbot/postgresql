# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Test for WAL source switch feature
use strict;
use warnings;

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;

$ENV{PGDATABASE} = 'postgres';

# Initialize primary node, setting wal-segsize to 1MB
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(
	allows_streaming => 1,
	has_archiving    => 1,
	extra            => ['--wal-segsize=1']);

# Ensure checkpoint doesn't come in our way
$node_primary->append_conf(
	'postgresql.conf', qq(
    min_wal_size = 2MB
    max_wal_size = 1GB
    checkpoint_timeout = 1h
    wal_recycle = off
));
$node_primary->start;

# Create an inactive replication slot to keep the WAL
$node_primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('rep1')");

# Take backup
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Create a standby linking to it using the replication slot
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup(
	$node_primary, $backup_name,
	has_streaming => 1,
	has_restoring => 1);
$node_standby->append_conf(
	'postgresql.conf', qq(
primary_slot_name = 'rep1'
min_wal_size = 2MB
max_wal_size = 1GB
checkpoint_timeout = 1h
streaming_replication_retry_interval = 10ms
wal_recycle = off
log_min_messages = 'debug1'
));

$node_standby->start;

# Wait until standby has replayed enough data
$node_primary->wait_for_catchup($node_standby);

$node_standby->stop;

# Advance WAL by 10 segments (= 10MB) on primary
advance_wal($node_primary, 10);

# Wait for primary to generate requested WAL files
$node_primary->poll_query_until('postgres',
	"SELECT COUNT(*) >= 10 FROM pg_ls_waldir();")
  or die "Timed out while waiting for primary to generate WAL";

# Wait until generated WAL files have been stored on the archives of the
# primary. This ensures that the standby created below will be able to restore
# the WAL files.
my $primary_archive = $node_primary->archive_dir;
$node_primary->poll_query_until('postgres',
	"SELECT COUNT(*) >= 10 FROM pg_ls_dir('$primary_archive', false, false) a WHERE a ~ '^[0-9A-F]{24}\$';"
) or die "Timed out while waiting for archiving of WAL by primary";

# Generate some data on the primary
$node_primary->safe_psql('postgres',
	"CREATE TABLE test_tbl AS SELECT a FROM generate_series(1,10) AS a;");

my $offset = -s $node_standby->logfile;

# Standby now connects to primary during inital recovery after fetching WAL
# from archive for about streaming_replication_retry_interval milliseconds.
$node_standby->start;

# Wait until standby has replayed enough data
$node_primary->wait_for_catchup($node_standby);

$node_standby->wait_for_log(
	qr/LOG: ( [A-Z0-9]+:)? restored log file ".*" from archive/, $offset);

$node_standby->wait_for_log(
	qr/DEBUG: ( [A-Z0-9]+:)? trying to switch WAL source to stream after fetching WAL from archive for at least 10 milliseconds/,
	$offset);

$node_standby->wait_for_log(
	qr/DEBUG: ( [A-Z0-9]+:)? switched WAL source to stream after fetching WAL from archive for at least 10 milliseconds/,
	$offset);

$node_standby->wait_for_log(
	qr/LOG: ( [A-Z0-9]+:)? started streaming WAL from primary at .* on timeline .*/,
	$offset);

# Check the streamed data on the standby
my $result =
  $node_standby->safe_psql('postgres', "SELECT COUNT(*) FROM test_tbl;");

is($result, '10', 'check streamed data on standby');

$node_standby->stop;
$node_primary->stop;

#####################################
# Advance WAL of $node by $n segments
sub advance_wal
{
	my ($node, $n) = @_;

	# Advance by $n segments (= (wal_segment_size * $n) bytes) on primary.
	for (my $i = 0; $i < $n; $i++)
	{
		$node->safe_psql('postgres',
			"CREATE TABLE t (); DROP TABLE t; SELECT pg_switch_wal();");
	}
	return;
}

done_testing();
