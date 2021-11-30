
# Copyright (c) 2021, PostgreSQL Global Development Group

# This tests that sequences are replicated correctly by logical replication
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 6;

# Initialize publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# Create some preexisting content on publisher
my $ddl = qq(
	CREATE SEQUENCE s;
);

# Setup structure on the publisher
$node_publisher->safe_psql('postgres', $ddl);

# Create some the same structure on subscriber, and an extra sequence that
# we'll create on the publisher later
$ddl = qq(
	CREATE SEQUENCE s;
	CREATE SEQUENCE s2;
);

$node_subscriber->safe_psql('postgres', $ddl);

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION seq_pub");

$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION seq_pub ADD SEQUENCE s");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION seq_sub CONNECTION '$publisher_connstr' PUBLICATION seq_pub WITH (slot_name = seq_sub_slot)"
);

$node_publisher->wait_for_catchup('seq_sub');

# Wait for initial sync to finish as well
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('s', 'r');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Insert initial test data
$node_publisher->safe_psql(
	'postgres', qq(
	-- generate a number of values using the sequence
	SELECT nextval('s') FROM generate_series(1,100);
));

$node_publisher->wait_for_catchup('seq_sub');

# Check the data on subscriber
my $result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT * FROM s;
));

is( $result, '132|0|t',
	'check replicated sequence values on subscriber');


# advance the sequence in a rolled-back transaction - should not be replicated
$node_publisher->safe_psql(
	'postgres', qq(
	BEGIN;
	SELECT nextval('s') FROM generate_series(1,100);
	ROLLBACK;
));

$node_publisher->wait_for_catchup('seq_sub');

# Check the data on subscriber
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT * FROM s;
));

is( $result, '132|0|t',
	'check replicated sequence values on subscriber');


# create a new sequence and roll it back - should not be replicated, due to
# the transactional behavior
$node_publisher->safe_psql(
	'postgres', qq(
	BEGIN;
	CREATE SEQUENCE s2;
	ALTER PUBLICATION seq_pub ADD SEQUENCE s2;
	SELECT nextval('s2') FROM generate_series(1,100);
	ROLLBACK;
));

$node_publisher->wait_for_catchup('seq_sub');

# Check the data on subscriber
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT * FROM s2;
));

is( $result, '1|0|f',
	'check replicated sequence values on subscriber');


# create a new sequence, advance it in a rolled-back transaction, but commit
# the create - the advance should be replicated nevertheless
$node_publisher->safe_psql(
	'postgres', qq(
	BEGIN;
	CREATE SEQUENCE s2;
	ALTER PUBLICATION seq_pub ADD SEQUENCE s2;
	SAVEPOINT sp1;
	SELECT nextval('s2') FROM generate_series(1,100);
	ROLLBACK TO sp1;
	COMMIT;
));

$node_publisher->wait_for_catchup('seq_sub');

# Wait for sync of the second sequence we just added to finish
$synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('s', 'r');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Check the data on subscriber
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT * FROM s2;
));

is( $result, '132|0|t',
	'check replicated sequence values on subscriber');


# advance the new sequence in a transaction, and roll it back - in this case
# it should not be replicated at commit
$node_publisher->safe_psql(
	'postgres', qq(
	BEGIN;
	SELECT nextval('s2') FROM generate_series(1,100);
	ROLLBACK;
));

$node_publisher->wait_for_catchup('seq_sub');

# Check the data on subscriber
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT * FROM s2;
));

is( $result, '132|0|t',
	'check replicated sequence values on subscriber');


# advance the sequence in a subtransaction - the subtransaction gets rolled
# back, but commit the main one - the changes should still be replicated
$node_publisher->safe_psql(
	'postgres', qq(
	BEGIN;
	SAVEPOINT s1;
	SELECT nextval('s2') FROM generate_series(1,100);
	ROLLBACK TO s1;
	COMMIT;
));

$node_publisher->wait_for_catchup('seq_sub');

# Check the data on subscriber
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT * FROM s2;
));

is( $result, '330|0|t',
	'check replicated sequence values on subscriber');


$node_subscriber->stop('fast');
$node_publisher->stop('fast');
