# Test behavior with streaming transaction exceeding logical_work_mem
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;

sub wait_for_caught_up
{
	my ($node, $appname) = @_;

	$node->poll_query_until('postgres',
"SELECT pg_current_wal_lsn() <= replay_lsn FROM pg_stat_replication WHERE application_name = '$appname';"
	) or die "Timed out while waiting for subscriber to catch up";
}

# Create publisher node
my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf', 'logical_work_mem = 64kB');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# Create some preexisting content on publisher
$node_publisher->safe_psql('postgres',
	"CREATE TABLE test_tab (a int primary key, b varchar)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab VALUES (1, 'foo'), (2, 'bar')");

# Setup structure on subscriber
$node_subscriber->safe_psql('postgres', "CREATE TABLE test_tab (a int primary key, b text, c INT, d INT, e INT)");

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres', "CREATE PUBLICATION tap_pub FOR TABLE test_tab");

my $appname = 'tap_sub';
$node_subscriber->safe_psql('postgres',
"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr application_name=$appname' PUBLICATION tap_pub"
);

wait_for_caught_up($node_publisher, $appname);

# Also wait for initial table sync to finish
my $synced_query =
"SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

my $result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*), count(c) FROM test_tab");
is($result, qq(2|0), 'check initial data was copied to subscriber');

# large (streamed) transaction with DDL, DML and ROLLBACKs
$node_publisher->safe_psql('postgres', q{
BEGIN;
INSERT INTO test_tab SELECT i, md5(i::text) FROM generate_series(3,500) s(i);
ALTER TABLE test_tab ADD COLUMN c INT;
SAVEPOINT s1;
INSERT INTO test_tab SELECT i, md5(i::text), -i FROM generate_series(501,1000) s(i);
ALTER TABLE test_tab ADD COLUMN d INT;
SAVEPOINT s2;
INSERT INTO test_tab SELECT i, md5(i::text), -i, 2*i FROM generate_series(1001,1500) s(i);
ALTER TABLE test_tab ADD COLUMN e INT;
SAVEPOINT s3;
INSERT INTO test_tab SELECT i, md5(i::text), -i, 2*i, -3*i FROM generate_series(1501,2000) s(i);
ALTER TABLE test_tab DROP COLUMN c;
ROLLBACK TO s1;
INSERT INTO test_tab SELECT i, md5(i::text), i FROM generate_series(501,1000) s(i);
COMMIT;
});

wait_for_caught_up($node_publisher, $appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*), count(c) FROM test_tab");
is($result, qq(1000|500), 'check extra columns contain local defaults');

$node_subscriber->stop;
$node_publisher->stop;
