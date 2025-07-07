# Copyright (c) 2025, PostgreSQL Global Development Group

# Test that a standby with archive recovery enabled does not start WAL receiver early:
# - Verifies that wal_receiver_start_at = 'consistency'
# - Ensures WAL receiver is not started when restore_command is specified

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use File::Copy;
use Test::More tests => 1;

# Initialize primary node with archving enabled and start it.
my $node_primary = PostgreSQL::Test::Cluster->new('test');
$node_primary->init(allows_streaming => 1, has_archiving => 1);
$node_primary->start;

# Initial workload.
$node_primary->safe_psql(
	'postgres', qq {
CREATE TABLE test_walreceiver_start(i int);
INSERT INTO test_walreceiver_start VALUES(1);
SELECT pg_switch_wal();
});

# Take backup.
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Run a post-backup workload
$node_primary->safe_psql(
	'postgres', qq {
INSERT INTO test_walreceiver_start VALUES(2);
SELECT pg_switch_wal();
});

# Initialize standby node from the backup with restore enabled from archived WAL
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1,  has_restoring => 1);

# Set up a long delay to prevent the standby from replaying past the first
# commit outside the backup.
$node_standby->append_conf('postgresql.conf',
	"recovery_min_apply_delay = '2h'");
# Set up the walreceiver to start as soon as consistency is reached.
$node_standby->append_conf('postgresql.conf',
	"wal_receiver_start_at = 'consistency'");

$node_standby->start();

# The standby should have reached consistency and should be blocked waiting for
# recovery_min_apply_delay.
$node_standby->poll_query_until(
	'postgres', qq{
SELECT wait_event = 'RecoveryApplyDelay' FROM pg_stat_activity
WHERE backend_type='startup';
}) or die "Timed out checking if startup is in recovery_min_apply_delay";

# The walreceiver should not have started
$node_standby->poll_query_until('postgres',
	"SELECT COUNT(1) = 1 FROM pg_stat_wal_receiver;", 'f')
	or die "Timed out while waiting for streaming to start";

# Now clear the delay so that the standby can replay the received WAL.
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET recovery_min_apply_delay TO 0;');
$node_standby->reload;

# Now the replay should catch up.
$node_primary->wait_for_catchup('standby', 'replay');
is( $node_standby->safe_psql(
	'postgres', 'SELECT count(*) FROM test_walreceiver_start;'),
	2,
	"querying test_walreceiver_start now should return 2 rows");
