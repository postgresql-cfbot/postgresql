# Copyright (c) 2024, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

##################################################
# Test race condition when timeout reached and new wal replayed on standby.
#
# This test relies on an injection point that cause to wait before Delete
# from heap and wait when new lsn added to heap.
##################################################

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Initialize primary node.
my $node_primary = PostgreSQL::Test::Cluster->new('master');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf(
	'postgresql.conf', q[
restart_after_crash = on
]);
$node_primary->start;

my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Setup first standby.
my $node_standby = PostgreSQL::Test::Cluster->new('standby1');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->start;

# Setup second standby.
my $node_standby2 = PostgreSQL::Test::Cluster->new('standby2');
$node_standby2->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby2->start;

# Create table
$node_primary->safe_psql('postgres', 'CREATE TABLE prim_tab (a int);');

# Create extention injection point
$node_primary->safe_psql('postgres', 'CREATE EXTENSION injection_points;');
# Wait until the extension has been created on the standby
$node_primary->wait_for_replay_catchup($node_standby);

# Attach the point in before deleteLSNWaiter.
$node_standby->safe_psql('postgres',
	"SELECT injection_points_attach('pg-wal-replay-wait', 'wait');");

# Get log offset
my $logstart = -s $node_standby->logfile;

# Get current lsn from primary
my $lsn_pr = $node_primary->safe_psql('postgres',
	"SELECT pg_current_wal_insert_lsn() + 10000");

# CALL pg_wal_replay_wait with timeout on standby1
my $psql_session1 = $node_standby->background_psql('postgres');
$psql_session1->query_until(
	qr/start/, qq[
	\\echo start
	CALL pg_wal_replay_wait('${lsn_pr}',1000);
]);

# Wait till pg-wal-replay-wait apperied in pg_stat_activity
$node_standby->wait_for_event('client backend', 'pg-wal-replay-wait');

# Generate some WAL
$node_primary->safe_psql('postgres', 'INSERT INTO prim_tab VALUES (1);');
$node_primary->wait_for_replay_catchup($node_standby);
$node_primary->wait_for_replay_catchup($node_standby2);

# CALL pg_wal_replay_wait with timeout on standby2
my $psql_session2 = $node_standby2->background_psql('postgres');
$psql_session2->query_until(
	qr/start/, qq[
	\\echo start
	CALL pg_wal_replay_wait('${lsn_pr}',1000);
]);

# Wake up to check the message
$node_standby->safe_psql('postgres',
	"SELECT injection_points_wakeup('pg-wal-replay-wait');");

# Check the log
ok( $node_standby->log_contains(
		"timed out while waiting for target LSN", $logstart),
	"pg_wal_replay_wait timeout");

done_testing();
