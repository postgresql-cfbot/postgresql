
# Copyright (c) 2022, PostgreSQL Global Development Group

# Test replication apply delay
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Create publisher node.
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	'logical_decoding_work_mem = 64kB');
$node_publisher->start;

# Create subscriber node.
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->append_conf('postgresql.conf',
	"log_min_messages = debug2");
$node_subscriber->start;

# Create some preexisting content on publisher.
$node_publisher->safe_psql('postgres',
	"CREATE TABLE test_tab (a int primary key, b varchar)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab VALUES (1, 'foo'), (2, 'bar')");

# Setup structure on subscriber.
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE test_tab (a int primary key, b text, c timestamptz DEFAULT now(), d bigint DEFAULT 999)"
);

# Setup logical replication.
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE test_tab");

my $appname = 'tap_sub';
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr application_name=$appname' PUBLICATION tap_pub WITH (streaming = on, min_apply_delay = '2s')"
);

# Wait for initial table sync to finish.
$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

# Check log starting now for logical replication apply delay.
my $log_location = -s $node_subscriber->logfile;

my $result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM test_tab");
is($result, qq(2|1|2), 'check initial data was copied to subscriber');

# New row to trigger apply delay.
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab VALUES (3, 'baz')");
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab VALUES (4, 'abc')");
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab VALUES (5, 'def')");

$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM test_tab");
is($result, qq(5|1|5), 'check if the new rows were applied to subscriber');

check_apply_delay_log("logical replication apply delay");

# Test streamed transaction.
# Insert, update and delete enough rows to exceed 64kB limit.
$node_publisher->safe_psql(
	'postgres', q{
BEGIN;
INSERT INTO test_tab SELECT i, md5(i::text) FROM generate_series(6, 5000) s(i);
UPDATE test_tab SET b = md5(b) WHERE mod(a, 2) = 0;
DELETE FROM test_tab WHERE mod(a, 3) = 0;
COMMIT;
});

$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM test_tab");
is($result, qq(3334|1|5000), 'check if the new rows were applied to subscriber');

check_apply_delay_log("logical replication apply delay");

# Test ALTER SUBSCRIPTION. Delay 86460 seconds (1 day 1 minute).
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub SET (min_apply_delay = 86460000)"
);

# New row to trigger apply delay.
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab VALUES (0, 'foobar')");

# Disable subscription. worker should die immediately.
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub DISABLE"
);

# Wait until worker dies.
my $sub_query =
  "SELECT count(1) = 0 FROM pg_stat_subscription WHERE subname = 'tap_sub' AND pid IS NOT NULL;";
$node_subscriber->poll_query_until('postgres', $sub_query)
  or die "Timed out while waiting for subscriber to die";

$node_subscriber->stop;
$node_publisher->stop;

done_testing();

sub check_apply_delay_log
{
	my $message          = shift;
	my $old_log_location = $log_location;

	$log_location = $node_subscriber->wait_for_log(qr/$message/, $log_location);

	cmp_ok($log_location, '>', $old_log_location,
		"logfile contains triggered logical replication apply delay"
	);
}
