
# Copyright (c) 2026, PostgreSQL Global Development Group

# Test that a cascading standby can reconnect to its upstream standby after
# advancing past the upstream's WAL flush position via archive recovery.
#
# Setup: primary -> standby_a -> standby_b
# standby_b has both streaming (from standby_a) and restore_command
# (from primary's archive).
#
# When standby_a's walreceiver is stopped and standby_b falls back to
# archive recovery, standby_b may advance its recovery position past
# standby_a's replay position.  Previously, standby_b's walreceiver
# would fail with "requested starting point is ahead of the WAL flush
# position" when reconnecting to standby_a.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize primary with archiving
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1, has_archiving => 1);
$node_primary->append_conf(
	'postgresql.conf', qq(
wal_keep_size = 128MB
checkpoint_timeout = 1h
));
$node_primary->start;

# Take backup and create standby_a (streaming from primary, no archive)
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

my $node_standby_a = PostgreSQL::Test::Cluster->new('standby_a');
$node_standby_a->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby_a->start;

# Wait for standby_a to start streaming
$node_primary->wait_for_catchup($node_standby_a);

# Take backup from standby_a and create standby_b
# standby_b streams from standby_a AND restores from primary's archive
$node_standby_a->backup($backup_name);

my $node_standby_b = PostgreSQL::Test::Cluster->new('standby_b');
$node_standby_b->init_from_backup($node_standby_a, $backup_name,
	has_streaming => 1);
$node_standby_b->enable_restoring($node_primary);
$node_standby_b->start;

# Generate initial data and wait for full cascade replication
$node_primary->safe_psql('postgres',
	"CREATE TABLE test_tab AS SELECT generate_series(1, 1000) AS id");
$node_primary->wait_for_replay_catchup($node_standby_a);
$node_standby_a->wait_for_replay_catchup($node_standby_b, $node_primary);

my $result =
  $node_standby_b->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, '1000', 'initial data replicated to cascading standby');

# Disconnect standby_a from primary by clearing primary_conninfo.
# This stops standby_a's walreceiver, so standby_a can no longer receive
# new WAL.  Its GetStandbyFlushRecPtr() will return only replayPtr.
$node_standby_a->append_conf('postgresql.conf', "primary_conninfo = ''");
$node_standby_a->reload;

# Wait for standby_a's walreceiver to stop
$node_standby_a->poll_query_until('postgres',
	"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_wal_receiver)")
  or die "Timed out waiting for standby_a walreceiver to stop";

# Stop standby_b cleanly.  We'll restart it after generating new WAL
# so it enters the recovery state machine fresh and tries archive first.
$node_standby_b->stop;

# Force a checkpoint now so that no background checkpoint can generate
# extra WAL during the INSERT below and push it across a segment boundary.
# Combined with checkpoint_timeout = 1h this ensures the new WAL fits
# within a single segment, keeping the gap within wal_segment_size.
$node_primary->safe_psql('postgres', "CHECKPOINT");

# Generate more WAL on primary
$node_primary->safe_psql('postgres',
	"INSERT INTO test_tab SELECT generate_series(1001, 2000)");

# Force WAL switch and wait for archiving to complete, so that
# standby_b can find the new WAL in the archive when it starts.
my $walfile = $node_primary->safe_psql('postgres',
	"SELECT pg_walfile_name(pg_current_wal_lsn())");
$node_primary->safe_psql('postgres', "SELECT pg_switch_wal()");
$node_primary->poll_query_until('postgres',
	"SELECT '$walfile' <= last_archived_wal FROM pg_stat_archiver")
  or die "Timed out waiting for WAL archiving";

# Rotate standby_b's log so we can check just the new log output
$node_standby_b->rotate_logfile;
my $standby_b_log_offset = -s $node_standby_b->logfile;

# Start standby_b.  It will:
# 1. Read new WAL from primary's archive (XLOG_FROM_ARCHIVE)
# 2. Advance RecPtr past standby_a's replay position
# 3. Try streaming from standby_a (XLOG_FROM_STREAM)
# 4. With the fix: walreceiver detects upstream is behind via
#    IDENTIFY_SYSTEM and waits instead of failing
$node_standby_b->start;

# Wait for standby_b to replay the new data from archive
$node_standby_b->poll_query_until('postgres',
	"SELECT count(*) >= 2000 FROM test_tab")
  or die "Timed out waiting for standby_b to replay archived WAL";

$result =
  $node_standby_b->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, '2000', 'cascading standby replayed new data from archive');

# Wait for walreceiver to hit the upstream-catchup wait event, proving we
# exercised the START_REPLICATION-ahead-of-upstream path.
$node_standby_b->wait_for_event('walreceiver', 'WalReceiverUpstreamCatchup');

# Verify no "requested starting point is ahead" errors occurred.
# Before the fix, standby_b's walreceiver would fail with this error
# when trying to reconnect to standby_a.
my $standby_b_log_contents =
  PostgreSQL::Test::Utils::slurp_file($node_standby_b->logfile,
	$standby_b_log_offset);
ok( $standby_b_log_contents !~
	  m/requested starting point .* is ahead of the WAL flush position/,
	'no "ahead of flush position" errors in standby_b log');

# Now restore standby_a's streaming from primary so it can catch up
$node_standby_a->enable_streaming($node_primary);
$node_standby_a->reload;

# Wait for standby_a to catch up with primary
$node_primary->wait_for_replay_catchup($node_standby_a);

# standby_b's walreceiver should eventually connect to standby_a and
# resume streaming (once standby_a has caught up past standby_b's position)
$node_standby_a->poll_query_until('postgres',
	"SELECT EXISTS (SELECT 1 FROM pg_stat_replication)")
  or die "Timed out waiting for standby_b to reconnect to standby_a";

# Verify end-to-end cascade streaming works with new data
$node_primary->safe_psql('postgres',
	"INSERT INTO test_tab SELECT generate_series(2001, 3000)");
$node_primary->wait_for_replay_catchup($node_standby_a);
$node_standby_a->wait_for_replay_catchup($node_standby_b, $node_primary);

$result =
  $node_standby_b->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, '3000',
	'cascade streaming resumes normally after upstream catches up');

done_testing();
