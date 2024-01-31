# Copyright (c) 2024, PostgreSQL Global Development Group
#
# Test for WAL source switch feature.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;

# Initialize primary node, setting wal-segsize to 1MB
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(
	allows_streaming => 1,
	has_archiving    => 1,
	extra            => ['--wal-segsize=1']);

# Ensure checkpoint doesn't come in our way
$node_primary->append_conf('postgresql.conf', qq(
    min_wal_size = 2MB
    max_wal_size = 1GB
    checkpoint_timeout = 1h
	autovacuum = off
));
$node_primary->start;

$node_primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('standby_slot')");

# Take backup
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Create a streaming standby
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup(
	$node_primary, $backup_name,
	has_streaming => 1,
	has_restoring => 1);
$node_standby->append_conf('postgresql.conf', qq(
	primary_slot_name = 'standby_slot'
	streaming_replication_retry_interval = 1ms
	log_min_messages = 'debug1'
));
$node_standby->start;

# Wait until standby has replayed enough data
$node_primary->wait_for_catchup($node_standby);

$node_standby->stop;

# Advance WAL by 5 segments (= 5MB) on primary
$node_primary->advance_wal(5);

# Wait for primary to generate requested WAL files
$node_primary->poll_query_until('postgres',
	"SELECT COUNT(*) >= 5 FROM pg_ls_waldir();")
  or die "Timed out while waiting for primary to generate WAL";

# Wait until generated WAL files have been stored on the archives of the
# primary. This ensures that the standby created below will be able to restore
# the WAL files.
my $primary_archive = $node_primary->archive_dir;
$node_primary->poll_query_until('postgres',
	"SELECT COUNT(*) >= 5 FROM pg_ls_dir('$primary_archive', false, false) a WHERE a ~ '^[0-9A-F]{24}\$';"
) or die "Timed out while waiting for archiving of WAL by primary";

# Generate some data on the primary
$node_primary->safe_psql('postgres',
	"CREATE TABLE test_tbl AS SELECT a FROM generate_series(1,5) AS a;");

my $offset = -s $node_standby->logfile;

# Standby now connects to primary during inital recovery after fetching WAL
# from archive for about streaming_replication_retry_interval milliseconds.
$node_standby->start;

# Wait until standby has replayed enough data
$node_primary->wait_for_catchup($node_standby);

$node_standby->wait_for_log(
	qr/DEBUG: ( [A-Z0-9]+:)? switched WAL source from archive to stream after timeout/,
	$offset);
$node_standby->wait_for_log(
	qr/LOG: ( [A-Z0-9]+:)? started streaming WAL from primary at .* on timeline .*/,
	$offset);

# Check that the data from primary is streamed to standby
my $result =
  $node_standby->safe_psql('postgres', "SELECT COUNT(*) FROM test_tbl;");
is($result, '5', 'data from primary is streamed to standby');

done_testing();
