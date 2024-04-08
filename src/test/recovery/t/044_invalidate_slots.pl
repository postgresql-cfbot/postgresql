# Copyright (c) 2024, PostgreSQL Global Development Group

# Test for replication slots invalidation
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;
use Time::HiRes qw(usleep);

# =============================================================================
# Testcase start: Invalidate streaming standby's slot as well as logical
# failover slot on primary due to replication_slot_inactive_timeout. Also,
# check the logical failover slot synced on to the standby doesn't invalidate
# the slot on its own, but gets the invalidated state from the remote slot on
# the primary.

# Initialize primary node
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 'logical');

# Avoid checkpoint during the test, otherwise, the test can get unpredictable
$primary->append_conf(
	'postgresql.conf', q{
checkpoint_timeout = 1h
autovacuum = off
});
$primary->start;

# Take backup
my $backup_name = 'my_backup';
$primary->backup($backup_name);

# Create a standby linking to the primary using the replication slot
my $standby1 = PostgreSQL::Test::Cluster->new('standby1');
$standby1->init_from_backup($primary, $backup_name, has_streaming => 1);

my $connstr_1 = $primary->connstr;
$standby1->append_conf(
	'postgresql.conf', qq(
hot_standby_feedback = on
primary_slot_name = 'sb1_slot'
primary_conninfo = '$connstr_1 dbname=postgres'
));

# Create sync slot on the primary
$primary->psql('postgres',
	q{SELECT pg_create_logical_replication_slot('lsub1_sync_slot', 'test_decoding', false, false, true);}
);

$primary->safe_psql(
	'postgres', qq[
    SELECT pg_create_physical_replication_slot(slot_name := 'sb1_slot');
]);

$standby1->start;

my $standby1_logstart = -s $standby1->logfile;

# Wait until standby has replayed enough data
$primary->wait_for_catchup($standby1);

# Synchronize the primary server slots to the standby
$standby1->safe_psql('postgres', "SELECT pg_sync_replication_slots();");

# Confirm that the logical failover slot is created on the standby and is
# flagged as 'synced'.
is( $standby1->safe_psql(
		'postgres',
		q{SELECT count(*) = 1 FROM pg_replication_slots
		  WHERE slot_name = 'lsub1_sync_slot' AND synced AND NOT temporary;}
	),
	"t",
	'logical slot has synced as true on standby');

my $logstart = -s $primary->logfile;
my $inactive_timeout = 2;

# Set timeout so that the next checkpoint will invalidate the inactive
# replication slot.
$primary->safe_psql(
	'postgres', qq[
    ALTER SYSTEM SET replication_slot_inactive_timeout TO '${inactive_timeout}s';
]);
$primary->reload;

# Wait for the logical failover slot to become inactive on the primary. Note
# that nobody has acquired that slot yet, so due to
# replication_slot_inactive_timeout setting above it must get invalidated.
wait_for_slot_invalidation($primary, 'lsub1_sync_slot', $logstart,
	$inactive_timeout);

# Set timeout on the standby also to check the synced slots don't get
# invalidated due to timeout on the standby.
$standby1->safe_psql(
	'postgres', qq[
    ALTER SYSTEM SET replication_slot_inactive_timeout TO '2s';
]);
$standby1->reload;

# Now, sync the logical failover slot from the remote slot on the primary.
# Note that the remote slot has already been invalidated due to inactive
# timeout. Now, the standby must also see it as invalidated.
$standby1->safe_psql('postgres', "SELECT pg_sync_replication_slots();");

# Wait for the inactive replication slot to be invalidated.
$standby1->poll_query_until(
	'postgres', qq[
	SELECT COUNT(slot_name) = 1 FROM pg_replication_slots
		WHERE slot_name = 'lsub1_sync_slot' AND
		invalidation_reason = 'inactive_timeout';
])
  or die
  "Timed out while waiting for replication slot lsub1_sync_slot invalidation to be synced on standby";

# Synced slot mustn't get invalidated on the standby even after a checkpoint,
# it must sync invalidation from the primary. So, we must not see the slot's
# invalidation message in server log.
$standby1->safe_psql('postgres', "CHECKPOINT");
ok( !$standby1->log_contains(
		"invalidating obsolete replication slot \"lsub1_sync_slot\"",
		$standby1_logstart),
	'check that syned slot has not been invalidated on the standby');

# Stop standby to make the standby's replication slot on the primary inactive
$standby1->stop;

# Wait for the standby's replication slot to become inactive
wait_for_slot_invalidation($primary, 'sb1_slot', $logstart,
	$inactive_timeout);

# Testcase end: Invalidate streaming standby's slot as well as logical failover
# slot on primary due to replication_slot_inactive_timeout. Also, check the
# logical failover slot synced on to the standby doesn't invalidate the slot on
# its own, but gets the invalidated state from the remote slot on the primary.
# =============================================================================

# =============================================================================
# Testcase start: Invalidate logical subscriber's slot due to
# replication_slot_inactive_timeout.

my $publisher = $primary;

# Prepare for the next test
$publisher->safe_psql(
	'postgres', qq[
    ALTER SYSTEM SET replication_slot_inactive_timeout TO '0';
]);
$publisher->reload;

# Create subscriber node
my $subscriber = PostgreSQL::Test::Cluster->new('sub');
$subscriber->init;
$subscriber->start;

# Create tables
$publisher->safe_psql('postgres', "CREATE TABLE test_tbl (id int)");
$subscriber->safe_psql('postgres', "CREATE TABLE test_tbl (id int)");

# Insert some data
$subscriber->safe_psql('postgres',
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

# Prepare for the next test
$publisher->safe_psql(
	'postgres', qq[
    ALTER SYSTEM SET replication_slot_inactive_timeout TO ' ${inactive_timeout}s';
]);
$publisher->reload;

$logstart = -s $publisher->logfile;

# Stop subscriber to make the replication slot on publisher inactive
$subscriber->stop;

# Wait for the replication slot to become inactive and then invalidated due to
# timeout.
wait_for_slot_invalidation($publisher, 'lsub1_slot', $logstart,
	$inactive_timeout);

# Testcase end: Invalidate logical subscriber's slot due to
# replication_slot_inactive_timeout.
# =============================================================================

sub wait_for_slot_invalidation
{
	my ($node, $slot_name, $offset, $inactive_timeout) = @_;
	my $name = $node->name;

	# Wait for the replication slot to become inactive
	$node->poll_query_until(
		'postgres', qq[
		SELECT COUNT(slot_name) = 1 FROM pg_replication_slots
			WHERE slot_name = '$slot_name' AND active = 'f';
	])
	  or die
	  "Timed out while waiting for replication slot to become inactive";

	# Wait for the replication slot info to be updated
	$node->poll_query_until(
		'postgres', qq[
		SELECT COUNT(slot_name) = 1 FROM pg_replication_slots
			WHERE inactive_since IS NOT NULL
				AND slot_name = '$slot_name' AND active = 'f';
	])
	  or die
	  "Timed out while waiting for info of replication slot $slot_name to be updated on node $name";

	# Sleep at least $inactive_timeout duration to avoid multiple checkpoints
	# for the slot to get invalidated.
	sleep($inactive_timeout);

	check_for_slot_invalidation_in_server_log($node, $slot_name, $offset);

	# Wait for the inactive replication slot to be invalidated
	$node->poll_query_until(
		'postgres', qq[
		SELECT COUNT(slot_name) = 1 FROM pg_replication_slots
			WHERE slot_name = '$slot_name' AND
			invalidation_reason = 'inactive_timeout';
	])
	  or die
	  "Timed out while waiting for inactive replication slot $slot_name to be invalidated on node $name";

	# Check that the invalidated slot cannot be acquired
	my ($result, $stdout, $stderr);

	($result, $stdout, $stderr) = $node->psql(
		'postgres', qq[
			SELECT pg_replication_slot_advance('$slot_name', '0/1');
	]);

	ok( $stderr =~
		  /can no longer get changes from replication slot "$slot_name"/,
		"detected error upon trying to acquire invalidated slot $slot_name on node $name"
	  )
	  or die
	  "could not detect error upon trying to acquire invalidated slot $slot_name";
}

# Check for invalidation of slot in server log
sub check_for_slot_invalidation_in_server_log
{
	my ($node, $slot_name, $offset) = @_;
	my $invalidated = 0;

	for (my $i = 0; $i < 10 * $PostgreSQL::Test::Utils::timeout_default; $i++)
	{
		$node->safe_psql('postgres', "CHECKPOINT");
		if ($node->log_contains(
				"invalidating obsolete replication slot \"$slot_name\"",
				$offset))
		{
			$invalidated = 1;
			last;
		}
		usleep(100_000);
	}
	ok($invalidated,
		"check that slot $slot_name invalidation has been logged");
}

done_testing();
