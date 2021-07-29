
# Copyright (c) 2021, PostgreSQL Global Development Group

# Test skipping logical replication transactions
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 8;

sub test_subscription_error
{
	my ($node, $expected, $source, $relname, $msg) = @_;

	# Wait for the error statistics to be updated.
	$node->poll_query_until(
	    'postgres', qq[
SELECT count(1) > 0 FROM pg_stat_subscription_errors
WHERE relid = '$relname'::regclass AND failure_source = '$source';
]) or die "Timed out while waiting for statistics to be updated";

	my $result = $node->safe_psql(
	    'postgres',
	    qq[
SELECT datname, subname, command, relid::regclass, failure_source, failure_count > 0
FROM pg_stat_subscription_errors
WHERE relid = '$relname'::regclass AND failure_source = '$source';
]);
	is($result, $expected, $msg);
}

# Create publisher node.
my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
			     'logical_decoding_work_mem = 64kB');
$node_publisher->start;

# Create subscriber node.
my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');

# don't overflow the server log with error messages.
$node_subscriber->append_conf('postgresql.conf',
			      'wal_retrieve_retry_interval = 5s');
$node_subscriber->start;

# Initial table setup on both publisher and subscriber. On subscriber we create
# the same tables but with primary keys. Also, insert some data that will conflict
# with the data replicated from publisher later.
$node_publisher->safe_psql('postgres',
			   q[
BEGIN;
CREATE TABLE test_tab1 (a int);
CREATE TABLE test_tab2 (a int);
CREATE TABLE test_tab_streaming (a int, b text);
INSERT INTO test_tab1 VALUES (1);
INSERT INTO test_tab2 VALUES (1);
COMMIT;
]);
$node_subscriber->safe_psql('postgres',
			    q[
BEGIN;
CREATE TABLE test_tab1 (a int primary key);
CREATE TABLE test_tab2 (a int primary key);
CREATE TABLE test_tab_streaming (a int primary key, b text);
INSERT INTO test_tab2 VALUES (1);
INSERT INTO test_tab_streaming SELECT 10000, md5(10000::text);
COMMIT;
]);

# Check if there is no subscription errors before starting logical replication.
my $result =
    $node_subscriber->safe_psql('postgres',
				"SELECT count(1) FROM pg_stat_subscription_errors");
is($result, qq(0), 'check no subscription error');

# Setup logical replication.
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
			   q[
CREATE PUBLICATION tap_pub FOR TABLE test_tab1, test_tab2;
CREATE PUBLICATION tap_pub_streaming FOR TABLE test_tab_streaming;
]);

# Start logical replication. The table sync for test_tab2 on tap_sub will enter
# infinite error due to violating the unique constraint.
my $appname = 'tap_sub';
$node_subscriber->safe_psql(
    'postgres',
    "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr application_name=$appname' PUBLICATION tap_pub WITH (two_phase = on);");
my $appname_streaming = 'tap_sub_streaming';
$node_subscriber->safe_psql(
    'postgres',
    "CREATE SUBSCRIPTION tap_sub_streaming CONNECTION '$publisher_connstr application_name=$appname_streaming' PUBLICATION tap_pub_streaming WITH (streaming = on);");


$node_publisher->wait_for_catchup($appname);
$node_publisher->wait_for_catchup($appname_streaming);

# Also wait for initial table sync for test_tab1 and test_tab_streaming to finish.
$node_subscriber->poll_query_until('postgres',
				   q[
SELECT count(1) = 2 FROM pg_subscription_rel
WHERE srrelid in ('test_tab1'::regclass, 'test_tab_streaming'::regclass) AND srsubstate = 'r'
]) or die "Timed out while waiting for subscriber to synchronize data";

$result = $node_subscriber->safe_psql('postgres',
				      "SELECT count(a) FROM test_tab1");
is($result, q(1), 'check initial data was copied to subscriber');

# Insert more data to test_tab1, raising an error on the subscriber due to violating
# the unique constraint on test_tab1.
$node_publisher->safe_psql('postgres',
			   "INSERT INTO test_tab1 VALUES (1)");

# Insert enough rows to test_tab_streaming to exceed the 64kB limit, also raising an
# error on the subscriber for the same reason.
$node_publisher->safe_psql('postgres',
			   "INSERT INTO test_tab_streaming SELECT i, md5(i::text) FROM generate_series(1, 10000) s(i);");

# Check both two errors on tap_sub subscription are reported.
test_subscription_error($node_subscriber, qq(postgres|tap_sub|INSERT|test_tab1|apply|t),
			'apply', 'test_tab1', 'error reporting by the apply worker');
test_subscription_error($node_subscriber, qq(postgres|tap_sub||test_tab2|tablesync|t),
			'tablesync', 'test_tab2', 'error reporting by the table sync worker');
test_subscription_error($node_subscriber, qq(postgres|tap_sub_streaming|INSERT|test_tab_streaming|apply|t),
			'apply', 'test_tab_streaming', 'error reporting by the apply worker');

# Set XIDs of the transactions in question to the subscriptions to skip.
my $skip_xid1 = $node_subscriber->safe_psql(
    'postgres',
    "SELECT xid FROM pg_stat_subscription_errors WHERE relid = 'test_tab1'::regclass");
my $skip_xid2 = $node_subscriber->safe_psql(
    'postgres',
    "SELECT xid FROM pg_stat_subscription_errors WHERE relid = 'test_tab_streaming'::regclass");

$node_subscriber->safe_psql('postgres',
			    "ALTER SUBSCRIPTION tap_sub SET (skip_xid = $skip_xid1)");
$node_subscriber->safe_psql('postgres',
			    "ALTER SUBSCRIPTION tap_sub_streaming SET (skip_xid = $skip_xid2)");

# Restart the subscriber to restart logical replication without interval.
$node_subscriber->restart;

# Wait for the transaction in question is skipped.
$node_subscriber->poll_query_until(
    'postgres',
    q[
SELECT count(1) = 2 FROM pg_subscription
WHERE subname in ('tap_sub', 'tap_sub_streaming') AND subskipxid IS NULL
]) or die "Timed out while waiting for the transaction to be skipped";

# Insert data to test_tab1 that doesn't conflict.
$node_publisher->safe_psql(
    'postgres',
    "INSERT INTO test_tab1 VALUES (2)");

# Also, insert data to test_tab_streaming that doesn't conflict.
$node_publisher->safe_psql(
    'postgres',
    "INSERT INTO test_tab_streaming VALUES (10001, md5(10001::text))");

$node_publisher->wait_for_catchup($appname);
$node_publisher->wait_for_catchup($appname_streaming);

# Check the data is successfully replicated after skipping the transaction.
$result = $node_subscriber->safe_psql('postgres',
				      "SELECT * FROM test_tab1");
is($result, q(1
2), "subscription gets changes after skipped transaction");
$result = $node_subscriber->safe_psql('postgres',
				      "SELECT count(1) FROM test_tab_streaming");
is($result, q(2), "subscription gets changes after skipped transaction");

# Check if the view doesn't show any entries after dropping the subscription.
$node_subscriber->safe_psql(
    'postgres',
    q[
DROP SUBSCRIPTION tap_sub;
DROP SUBSCRIPTION tap_sub_streaming;
]);
$result = $node_subscriber->safe_psql('postgres',
				      "SELECT count(1) FROM pg_stat_subscription_errors");
is($result, q(0), 'no error after dropping subscription');
