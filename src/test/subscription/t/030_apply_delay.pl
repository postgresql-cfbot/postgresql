
# Copyright (c) 2022, PostgreSQL Global Development Group

# Test replication apply delay
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Create publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->append_conf('postgresql.conf',
	"log_min_messages = debug2");
$node_subscriber->start;

# Create some preexisting content on publisher
$node_publisher->safe_psql('postgres',
	"CREATE TABLE test_tab (a int primary key, b varchar)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab VALUES (1, 'foo'), (2, 'bar')");

# Setup structure on subscriber
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE test_tab (a int primary key, b text, c timestamptz DEFAULT now(), d bigint DEFAULT 999)"
);

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE test_tab");

my $appname = 'tap_sub';
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr application_name=$appname' PUBLICATION tap_pub WITH (min_apply_delay = '2s')"
);

$node_publisher->wait_for_catchup($appname);

# Also wait for initial table sync to finish
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

my $result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM test_tab");
is($result, qq(2|1|2), 'check initial data was copied to subscriber');

# new row to trigger apply delay
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab VALUES (3, 'baz')");

$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM test_tab");
is($result, qq(3|1|3), 'check the new row was applied to subscriber');

my $logfile = slurp_file($node_subscriber->logfile());
ok( $logfile =~
	  qr/logical replication apply delay/,
	'check if replication apply delay is triggered');

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
