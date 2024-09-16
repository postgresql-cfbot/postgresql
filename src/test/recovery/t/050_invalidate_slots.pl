# Copyright (c) 2024, PostgreSQL Global Development Group

# Test for replication slots invalidation
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;
use Time::HiRes qw(usleep);

# =============================================================================
# Testcase start
#
# Test invalidation of streaming standby slot and logical failover slot on the
# primary due to inactive timeout. Also, test logical failover slot synced to
# the standby from the primary doesn't get invalidated on its own, but gets the
# invalidated state from the primary.

# Initialize primary
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 'logical');

# Avoid unpredictability
$primary->append_conf(
	'postgresql.conf', qq{
checkpoint_timeout = 1h
autovacuum = off
});
$primary->start;

# Take backup
my $backup_name = 'my_backup';
$primary->backup($backup_name);

# Create standby
my $standby1 = PostgreSQL::Test::Cluster->new('standby1');
$standby1->init_from_backup($primary, $backup_name, has_streaming => 1);

my $connstr = $primary->connstr;
$standby1->append_conf(
	'postgresql.conf', qq(
hot_standby_feedback = on
primary_slot_name = 'sb_slot1'
primary_conninfo = '$connstr dbname=postgres'
));

# Create sync slot on the primary
$primary->psql('postgres',
	q{SELECT pg_create_logical_replication_slot('sync_slot1', 'test_decoding', false, false, true);}
);

# Create standby slot on the primary
$primary->safe_psql(
	'postgres', qq[
    SELECT pg_create_physical_replication_slot(slot_name := 'sb_slot1', immediately_reserve := true);
]);

$standby1->start;

# Wait until the standby has replayed enough data
$primary->wait_for_catchup($standby1);

my $logstart = -s $standby1->logfile;

# Set timeout GUC on the standby to verify that the next checkpoint will not
# invalidate synced slots.
my $inactive_timeout = 1;
$standby1->safe_psql(
	'postgres', qq[
    ALTER SYSTEM SET replication_slot_inactive_timeout TO '${inactive_timeout}s';
]);
$standby1->reload;

# Sync the primary slots to the standby
$standby1->safe_psql('postgres', "SELECT pg_sync_replication_slots();");

# Confirm that the logical failover slot is created on the standby
is( $standby1->safe_psql(
		'postgres',
		q{SELECT count(*) = 1 FROM pg_replication_slots
		  WHERE slot_name = 'sync_slot1' AND synced
			AND NOT temporary
			AND invalidation_reason IS NULL;}
	),
	"t",
	'logical slot sync_slot1 is synced to standby');

# Give enough time
sleep($inactive_timeout+1);

# Despite inactive timeout being set, the synced slot won't get invalidated on
# its own on the standby. So, we must not see invalidation message in server
# log.
$standby1->safe_psql('postgres', "CHECKPOINT");
ok( !$standby1->log_contains(
		"invalidating obsolete replication slot \"sync_slot1\"",
		$logstart),
	'check that synced slot sync_slot1 has not been invalidated on standby'
);

$logstart = -s $primary->logfile;

# Set timeout GUC so that the next checkpoint will invalidate inactive slots
$primary->safe_psql(
	'postgres', qq[
    ALTER SYSTEM SET replication_slot_inactive_timeout TO '${inactive_timeout}s';
]);
$primary->reload;

# Wait for logical failover slot to become inactive on the primary. Note that
# nobody has acquired the slot yet, so it must get invalidated due to
# inactive timeout.
wait_for_slot_invalidation($primary, 'sync_slot1', $logstart,
	$inactive_timeout);

# Re-sync the primary slots to the standby. Note that the primary slot was
# already invalidated (above) due to inactive timeout. The standby must just
# sync the invalidated state.
$standby1->safe_psql('postgres', "SELECT pg_sync_replication_slots();");
$standby1->poll_query_until(
	'postgres', qq[
	SELECT COUNT(slot_name) = 1 FROM pg_replication_slots
		WHERE slot_name = 'sync_slot1' AND
		invalidation_reason = 'inactive_timeout';
])
  or die
  "Timed out while waiting for sync_slot1 invalidation to be synced on standby";

# Make the standby slot on the primary inactive and check for invalidation
$standby1->stop;
wait_for_slot_invalidation($primary, 'sb_slot1', $logstart,
	$inactive_timeout);

# Testcase end
# =============================================================================

# =============================================================================
# Testcase start
# Invalidate logical subscriber slot due to inactive timeout.

my $publisher = $primary;

# Prepare for test
$publisher->safe_psql(
	'postgres', qq[
    ALTER SYSTEM SET replication_slot_inactive_timeout TO '0';
]);
$publisher->reload;

# Create subscriber
my $subscriber = PostgreSQL::Test::Cluster->new('sub');
$subscriber->init;
$subscriber->start;

# Create tables
$publisher->safe_psql('postgres', "CREATE TABLE test_tbl (id int)");
$subscriber->safe_psql('postgres', "CREATE TABLE test_tbl (id int)");

# Insert some data
$publisher->safe_psql('postgres',
	"INSERT INTO test_tbl VALUES (generate_series(1, 5));");

# Setup logical replication
my $publisher_connstr = $publisher->connstr . ' dbname=postgres';
$publisher->safe_psql('postgres', "CREATE PUBLICATION pub FOR ALL TABLES");
$publisher->safe_psql(
	'postgres', qq[
    SELECT pg_create_logical_replication_slot(slot_name := 'lsub1_slot', plugin := 'pgoutput');
]);

$subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub CONNECTION '$publisher_connstr' PUBLICATION pub WITH (slot_name = 'lsub1_slot', create_slot = false)"
);
$subscriber->wait_for_subscription_sync($publisher, 'sub');
my $result =
  $subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tbl");
is($result, qq(5), "check initial copy was done");

# Set timeout GUC so that the next checkpoint will invalidate inactive slots
$publisher->safe_psql(
	'postgres', qq[
    ALTER SYSTEM SET replication_slot_inactive_timeout TO ' ${inactive_timeout}s';
]);
$publisher->reload;

$logstart = -s $publisher->logfile;

# Make subscriber slot on publisher inactive and check for invalidation
$subscriber->stop;
wait_for_slot_invalidation($publisher, 'lsub1_slot', $logstart,
	$inactive_timeout);

# Testcase end
# =============================================================================

# Wait for slot to first become inactive and then get invalidated
sub wait_for_slot_invalidation
{
	my ($node, $slot, $offset, $inactive_timeout) = @_;
	my $node_name = $node->name;

	# Wait for slot to become inactive
	$node->poll_query_until(
		'postgres', qq[
		SELECT COUNT(slot_name) = 1 FROM pg_replication_slots
			WHERE slot_name = '$slot' AND active = 'f' AND
				  inactive_since IS NOT NULL;
	])
	  or die
	  "Timed out while waiting for slot $slot to become inactive on node $node_name";

	trigger_slot_invalidation($node, $slot, $offset, $inactive_timeout);

	# Check that an invalidated slot cannot be acquired
	my ($result, $stdout, $stderr);
	($result, $stdout, $stderr) = $node->psql(
		'postgres', qq[
			SELECT pg_replication_slot_advance('$slot', '0/1');
	]);
	ok( $stderr =~
		  /can no longer get changes from replication slot "$slot"/,
		"detected error upon trying to acquire invalidated slot $slot on node $node_name"
	  )
	  or die
	  "could not detect error upon trying to acquire invalidated slot $slot on node $node_name";
}

# Trigger slot invalidation and confirm it in the server log
sub trigger_slot_invalidation
{
	my ($node, $slot, $offset, $inactive_timeout) = @_;
	my $node_name = $node->name;
	my $invalidated = 0;

	# Give enough time to avoid multiple checkpoints
	sleep($inactive_timeout + 1);

	for (my $i = 0; $i < 10 * $PostgreSQL::Test::Utils::timeout_default; $i++)
	{
		$node->safe_psql('postgres', "CHECKPOINT");
		if ($node->log_contains(
				"invalidating obsolete replication slot \"$slot\"",
				$offset))
		{
			$invalidated = 1;
			last;
		}
		usleep(100_000);
	}
	ok($invalidated,
		"check that slot $slot invalidation has been logged on node $node_name"
	);

	# Check that the invalidation reason is 'inactive_timeout'
	$node->poll_query_until(
		'postgres', qq[
		SELECT COUNT(slot_name) = 1 FROM pg_replication_slots
			WHERE slot_name = '$slot' AND
			invalidation_reason = 'inactive_timeout';
	])
	  or die
	  "Timed out while waiting for invalidation reason of slot $slot to be set on node $node_name";
}

done_testing();
