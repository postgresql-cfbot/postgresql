
# Copyright (c) 2021, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
my $node_phys_standby = PostgreSQL::Test::Cluster->new('phys_standby');
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');

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

# Check invalidation in the logfile
sub check_for_invalidation
{
	my ($log_start, $test_name) = @_;

	# message should be issued
	ok( find_in_log(
		$node_phys_standby,
        "invalidating obsolete replication slot \"sub1\"", $log_start),
        "sub1 slot invalidation is logged $test_name");
}

# Check conflicting status in pg_replication_slots.
sub check_slots_conflicting_status
{
	my $res = $node_phys_standby->safe_psql(
				'postgres', qq(
				select bool_and(conflicting) from pg_replication_slots;));

	is($res, 't',
		"Logical slot is reported as conflicting");
}

$node_primary->init(allows_streaming => 'logical');
$node_primary->append_conf('postgresql.conf', q{
synchronize_slot_names = '*'
standby_slot_names = 'pslot1'
});
$node_primary->start;
$node_primary->psql('postgres', q{SELECT pg_create_physical_replication_slot('pslot1');});

$node_primary->backup('backup');

$node_phys_standby->init_from_backup($node_primary, 'backup', has_streaming => 1);
$node_phys_standby->append_conf('postgresql.conf', q{
synchronize_slot_names = '*'
primary_slot_name = 'pslot1'
hot_standby_feedback = off
});
$node_phys_standby->start;

$node_primary->safe_psql('postgres', "CREATE TABLE t1 (a int PRIMARY KEY)");
$node_primary->safe_psql('postgres', "INSERT INTO t1 VALUES (1), (2), (3)");

# Some tests need to wait for VACUUM to be replayed. But vacuum does not flush
# WAL. An insert into flush_wal outside transaction does guarantee a flush.
$node_primary->psql('postgres', q[CREATE TABLE flush_wal();]);

$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

$node_subscriber->safe_psql('postgres', "CREATE TABLE t1 (a int PRIMARY KEY)");

$node_primary->safe_psql('postgres', "CREATE PUBLICATION pub1 FOR TABLE t1");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub1 CONNECTION '" . ($node_primary->connstr . ' dbname=postgres') . "' PUBLICATION pub1");

# Wait for initial sync of all subscriptions
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

my $result = $node_primary->safe_psql('postgres',
	"SELECT slot_name, plugin, database FROM pg_replication_slots WHERE slot_type = 'logical'");

is($result, qq(sub1|pgoutput|postgres), 'logical slot on primary');

# FIXME: standby needs restart to pick up new slots
$node_phys_standby->restart;
sleep 3;

$result = $node_phys_standby->safe_psql('postgres',
	"SELECT slot_name, plugin, database FROM pg_replication_slots");

is($result, qq(sub1|pgoutput|postgres), 'logical slot on standby');

$node_primary->safe_psql('postgres', "INSERT INTO t1 VALUES (4), (5), (6)");
$node_primary->wait_for_catchup('sub1');

$node_primary->wait_for_catchup($node_phys_standby->name);

# Logical subscriber and physical replica are caught up at this point.

# Drop the subscription so that catalog_xmin is unknown on the primary
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION sub1");

# This should trigger a conflict as hot_standby_feedback is off on the standby
$node_primary->safe_psql('postgres', qq[
  CREATE TABLE conflict_test(x integer, y text);
  DROP TABLE conflict_test;
  VACUUM full pg_class;
  INSERT INTO flush_wal DEFAULT VALUES; -- see create table flush_wal
]);

# Ensure physical replay catches up
$node_primary->wait_for_catchup($node_phys_standby);

# Check invalidation in the logfile
check_for_invalidation(1, 'with vacuum FULL on pg_class');

# Check conflicting status in pg_replication_slots.
check_slots_conflicting_status();

done_testing();
