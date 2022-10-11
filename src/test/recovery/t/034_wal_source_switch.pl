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
    has_archiving => 1,
    extra => ['--wal-segsize=1']);
# Ensure checkpoint doesn't come in our way
$node_primary->append_conf(
	'postgresql.conf', qq(
    min_wal_size = 2MB
    max_wal_size = 1GB
    checkpoint_timeout = 1h
    wal_recycle = off
));
$node_primary->start;
$node_primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('rep1')");

# Take backup
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Create a standby linking to it using the replication slot
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1,
    has_restoring => 1);
$node_standby->append_conf(
	'postgresql.conf', qq(
primary_slot_name = 'rep1'
min_wal_size = 2MB
max_wal_size = 1GB
checkpoint_timeout = 1h
streaming_replication_retry_interval = 100ms
wal_recycle = off
log_min_messages = 'debug2'
));

$node_standby->start;

# Wait until standby has replayed enough data
$node_primary->wait_for_catchup($node_standby);

# Stop standby
$node_standby->stop;

# Advance WAL by 100 segments (= 100MB) on primary
advance_wal($node_primary, 100);

# Wait for primary to generate requested WAL files
$node_primary->poll_query_until('postgres',
	q|SELECT COUNT(*) >= 100 FROM pg_ls_waldir()|, 't');

# Standby now connects to primary during inital recovery after fetching WAL
# from archive for about streaming_replication_retry_interval milliseconds.
$node_standby->start;

$node_primary->wait_for_catchup($node_standby);

ok(find_in_log(
		$node_standby,
        qr/restored log file ".*" from archive/),
	    'check that some of WAL segments were fetched from archive');

ok(find_in_log(
		$node_standby,
        qr/trying to switch WAL source to .* after fetching WAL from .* for at least .* milliseconds/),
	    'check that standby tried to switch WAL source to primary from archive');

ok(find_in_log(
		$node_standby,
        qr/switched WAL source to .* after fetching WAL from .* for at least .* milliseconds/),
	    'check that standby actually switched WAL source to primary from archive');

ok(find_in_log(
		$node_standby,
        qr/started streaming WAL from primary at .* on timeline .*/),
	    'check that standby strated streaming from primary');

# Stop standby
$node_standby->stop;

# Stop primary
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

# find $pat in logfile of $node after $off-th byte
sub find_in_log
{
	my ($node, $pat, $off) = @_;

	$off = 0 unless defined $off;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
	return 0 if (length($log) <= $off);

	$log = substr($log, $off);

	return $log =~ m/$pat/;
}

done_testing();
