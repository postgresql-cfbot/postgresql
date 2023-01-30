
# Copyright (c) 2023, PostgreSQL Global Development Group

# Test replication apply delay
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Confirm the time-delayed replication has been effective from the server log
# message where the apply worker emits for applying delay. Moreover, verify
# that the current worker's remaining wait time is sufficiently bigger than the
# expected value, in order to check any update of the min_apply_delay.
sub check_apply_delay_log
{
	my ($node_subscriber, $offset, $expected) = @_;

	my $log_location = $node_subscriber->wait_for_log(
		qr/time-delayed replication for txid (\d+), min_apply_delay = (\d+) ms, remaining wait time: (\d+) ms/,
		$offset);

	cmp_ok($log_location, '>', $offset,
		"logfile contains triggered logical replication apply delay");

	# Get the remaining wait time from the server log
	my $contents = slurp_file($node_subscriber->logfile, $offset);
	$contents =~
	  qr/time-delayed replication for txid (\d+), min_apply_delay = (\d+) ms, remaining wait time: (\d+) ms/,
	  or die "could not get the apply worker wait time";
	my $logged_delay = $3;

	# Is it larger than expected?
	cmp_ok($logged_delay, '>', $expected,
		"The apply worker wait time has expected duration");
}

# Compare inserted time on the publisher with applied time on the subscriber to
# confirm the latter is applied after expected time. The time is automatically
# generated and stored in the table column 'c'.
sub check_apply_delay_time
{
	my ($node_publisher, $node_subscriber, $primary_key, $expected_diffs) =
	  @_;

	my $inserted_time_on_pub = $node_publisher->safe_psql(
		'postgres', qq[
		SELECT extract(epoch from c) FROM test_tab WHERE a = $primary_key;
	]);

	my $inserted_time_on_sub = $node_subscriber->safe_psql(
		'postgres', qq[
		SELECT extract(epoch from c) FROM test_tab WHERE a = $primary_key;
	]);

	cmp_ok(
		$inserted_time_on_sub - $inserted_time_on_pub,
		'>',
		$expected_diffs,
		"The tuple on the subscriber was modified later than the publisher");
}

# Create publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	'logical_decoding_work_mem = 64kB');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->append_conf('postgresql.conf', "log_min_messages = debug2");
$node_subscriber->start;

# Setup structure on publisher
$node_publisher->safe_psql('postgres',
	"CREATE TABLE test_tab (a int primary key, b varchar, c timestamptz (6) DEFAULT now())"
);

# Setup structure on subscriber
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE test_tab (a int primary key, b text, c timestamptz (6) DEFAULT now())"
);

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

# The column 'c' must not be published because we want to compare the time
# difference.
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE test_tab (a, b)");

my $appname = 'tap_sub';

# Create a subscription that applies the transaction after 1 second delay
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr application_name=$appname' PUBLICATION tap_pub WITH (copy_data = off, min_apply_delay = '1s', streaming = 'on')"
);

# Check log starting now for logical replication apply delay
my $offset = -s $node_subscriber->logfile;

# New row to trigger apply delay
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab VALUES (1, 'foo')");
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab VALUES (2, 'bar')");

$node_publisher->wait_for_catchup($appname);

my $result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM test_tab");
is($result, qq(2|1|2), 'check if the new rows were applied to subscriber');

# Make sure the apply worker knows to wait for more than 500ms
check_apply_delay_log($node_subscriber, $offset, "0.5");

# Verify that the subscriber lags the publisher by at least 1 second
check_apply_delay_time($node_publisher, $node_subscriber, '2', '1');

# Setup for streaming case
$node_publisher->append_conf('postgresql.conf',
	'logical_replication_mode = immediate');
$node_publisher->reload;

# Run a query to make sure that the reload has taken effect.
$node_publisher->safe_psql('postgres', q{SELECT 1});

# Check log starting now for logical replication apply delay
$offset = -s $node_subscriber->logfile;

# Test streamed transaction by insert
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab SELECT i, md5(i::text) FROM generate_series(3, 5) s(i);"
);

$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM test_tab");
is($result, qq(5|1|5), 'check if the new rows were applied to subscriber');

# Make sure the apply worker knows to wait for more than 500ms
check_apply_delay_log($node_subscriber, $offset, "0.5");

# Verify that the subscriber lags the publisher by at least 1 second
check_apply_delay_time($node_publisher, $node_subscriber, '5', '1');

# Test whether ALTER SUBSCRIPTION changes the delayed time of the apply worker
# (1 day 5 minutes). Note that the extra 5 minute is to account for any
# decoding/network overhead.
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub SET (min_apply_delay = 86700000)");

# Check log starting now for logical replication apply delay
$offset = -s $node_subscriber->logfile;

# New row to trigger apply delay
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab VALUES (0, 'foobar')");

# Make sure the apply worker knows to wait for more than 1 day
check_apply_delay_log($node_subscriber, $offset, "86400000");

# Disable subscription and the worker should die immediately
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub DISABLE;");

# Wait until worker dies
my $sub_query =
  "SELECT count(1) = 0 FROM pg_stat_subscription WHERE subname = 'tap_sub' AND pid IS NOT NULL;";
$node_subscriber->poll_query_until('postgres', $sub_query)
  or die "Timed out while waiting for subscriber to die";

# Confirm disabling the subscription by ALTER SUBSCRIPTION DISABLE did not cause
# the delayed transaction to be applied.
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(a) FROM test_tab WHERE a = 0;");
is($result, qq(0), "check the delayed transaction was not applied");

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
