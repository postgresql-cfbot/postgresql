# This tests that the errors when data type conversion are correctly
# handled by logical replication apply workers
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 1;

# Initialize publisher node
my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# Setup same table by different steps so that publisher
# and subscriber get different OID of the dummytext data.
# It's necessary for checking if the subscriber can correcly
# look up both remote and local data type strings.
my $ddl = qq(
CREATE EXTENSION test_subscription;
CREATE TABLE test (a dummytext););

$node_publisher->safe_psql('postgres', qq(
CREATE EXTENSION hstore;));
$node_publisher->safe_psql('postgres', $ddl);
$node_subscriber->safe_psql('postgres', $ddl);

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres', qq(
CREATE PUBLICATION tap_pub FOR TABLE test
));
my $appname = 'tap_sub';
$node_subscriber->safe_psql('postgres', qq(
CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr application_name=$appname' PUBLICATION tap_pub WITH (slot_name = tap_sub_slot, copy_data = false)
));

# Insert test data.which will lead to call the callback
# function for the date type conversion on subscriber.
$node_publisher->safe_psql('postgres', qq(
INSERT INTO test VALUES ('1');
));

$node_publisher->wait_for_catchup($appname);

# Check the data on subscriber
my $result = $node_subscriber->safe_psql('postgres', qq(
SELECT a FROM test;
));

# Inserted data is replicated correctly
is( $result, '1');
