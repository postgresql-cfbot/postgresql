
# Copyright (c) 2021, PostgreSQL Global Development Group

# Basic logical replication test
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 14;

# Initialize publisher node
my $node_publisher = PostgresNode->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = PostgresNode->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# Test replication with publications created using FOR ALL TABLES IN SCHEMA
# option.
# Create schemas and tables on publisher
$node_publisher->safe_psql('postgres', "CREATE SCHEMA sch1");
$node_publisher->safe_psql('postgres', "CREATE SCHEMA sch2");
$node_publisher->safe_psql('postgres', "CREATE SCHEMA sch3");
$node_publisher->safe_psql('postgres', "CREATE TABLE sch1.tab1 AS SELECT generate_series(1,10) AS a");
$node_publisher->safe_psql('postgres', "CREATE TABLE sch1.tab2 AS SELECT generate_series(1,10) AS a");
$node_publisher->safe_psql('postgres', "CREATE TABLE sch2.tab1 AS SELECT generate_series(1,10) AS a");
$node_publisher->safe_psql('postgres', "CREATE TABLE sch2.tab2 AS SELECT generate_series(1,10) AS a");

# Create schemas and tables on subscriber
$node_subscriber->safe_psql('postgres', "CREATE SCHEMA sch1");
$node_subscriber->safe_psql('postgres', "CREATE SCHEMA sch2");
$node_subscriber->safe_psql('postgres', "CREATE SCHEMA sch3");
$node_subscriber->safe_psql('postgres', "CREATE TABLE sch1.tab1 (a int)");
$node_subscriber->safe_psql('postgres', "CREATE TABLE sch1.tab2 (a int)");
$node_subscriber->safe_psql('postgres', "CREATE TABLE sch2.tab1 (a int)");
$node_subscriber->safe_psql('postgres', "CREATE TABLE sch2.tab2 (a int)");

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_schema FOR ALL TABLES IN SCHEMA sch1,sch2");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub_schema CONNECTION '$publisher_connstr' PUBLICATION tap_pub_schema"
	);

$node_publisher->wait_for_catchup('tap_sub_schema');

# Also wait for initial table sync to finish
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Check the schema table data is synced up.
my $result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM sch1.tab1");
is($result, qq(10|1|10), 'check rows on subscriber catchup');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM sch1.tab2");
is($result, qq(10|1|10), 'check rows on subscriber catchup');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM sch2.tab1");
is($result, qq(10|1|10), 'check rows on subscriber catchup');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM sch2.tab2");
is($result, qq(10|1|10), 'check rows on subscriber catchup');

# Insert some data into few tables and verify that inserted data is replicated.
$node_publisher->safe_psql('postgres', "INSERT INTO sch1.tab1 VALUES(generate_series(11,20))");
$node_publisher->safe_psql('postgres', "INSERT INTO sch2.tab1 VALUES(generate_series(11,20))");

$node_publisher->wait_for_catchup('tap_sub_schema');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM sch1.tab1");
is($result, qq(20|1|20), 'check rows on subscriber catchup');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM sch2.tab1");
is($result, qq(20|1|20), 'check rows on subscriber catchup');

# Create new table in the publication schema, verify that subscriber does not get
# the new table data in the subscriber before refresh.
$node_publisher->safe_psql('postgres', "CREATE TABLE sch1.tab3 AS SELECT generate_series(1,10) AS a");
$node_subscriber->safe_psql('postgres', "CREATE TABLE sch1.tab3(a int)");
$node_publisher->wait_for_catchup('tap_sub_schema');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM sch1.tab3");
is($result, qq(0), 'check rows on subscriber catchup');

# Table data should be reflected after refreshing the publication in
# subscriber.
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub_schema REFRESH PUBLICATION");

# Also wait for initial table sync to finish
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

$node_publisher->safe_psql('postgres', "INSERT INTO sch1.tab3 VALUES(11)");
$node_publisher->wait_for_catchup('tap_sub_schema');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM sch1.tab3");
is($result, qq(11|1|11), 'check rows on subscriber catchup');

# Set the schema of a publication schema table to a non publication schema and
# verify that inserted data is not reflected by the subscriber.
$node_publisher->safe_psql('postgres', "ALTER TABLE sch1.tab3 SET SCHEMA sch3");
$node_publisher->safe_psql('postgres', "INSERT INTO sch3.tab3 VALUES(11)");
$node_publisher->wait_for_catchup('tap_sub_schema');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM sch1.tab3");
is($result, qq(11|1|11), 'check rows on subscriber catchup');

# Verify that the subscription relation list is updated after refresh.
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_subscription_rel WHERE srsubid IN (SELECT oid FROM pg_subscription WHERE subname = 'tap_sub_schema')");
is($result, qq(5),
	'check subscription relation status is not yet dropped on subscriber');
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub_schema REFRESH PUBLICATION");
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_subscription_rel WHERE srsubid IN (SELECT oid FROM pg_subscription WHERE subname = 'tap_sub_schema')");
is($result, qq(4),
	'check subscription relation status was dropped on subscriber');

# Drop table from the publication schema, verify that subscriber removes the
# table entry after refresh.
$node_publisher->safe_psql('postgres', "DROP TABLE sch1.tab2");
$node_publisher->wait_for_catchup('tap_sub_schema');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_subscription_rel WHERE srsubid IN (SELECT oid FROM pg_subscription WHERE subname = 'tap_sub_schema')");
is($result, qq(4),
	'check subscription relation status is not yet dropped on subscriber');

# Table should be removed from pg_subscription_rel after refreshing the
# publication in subscriber.
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub_schema REFRESH PUBLICATION");
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_subscription_rel WHERE srsubid IN (SELECT oid FROM pg_subscription WHERE subname = 'tap_sub_schema')");
is($result, qq(3),
	'check subscription relation status was dropped on subscriber');

# Drop schema from publication, verify that the inserts are not published after
# dropping the schema from publication. Here 2nd insert should not be
# published.
$node_publisher->safe_psql('postgres', "INSERT INTO sch2.tab1 VALUES(21); ALTER PUBLICATION tap_pub_schema DROP ALL TABLES IN SCHEMA sch2; INSERT INTO sch2.tab1 values(22)");
$node_publisher->wait_for_catchup('tap_sub_schema');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM sch2.tab1");
is($result, qq(21|1|21), 'check rows on subscriber catchup');

# Drop subscription as we don't need it anymore
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub_schema");

# Drop publication as we don't need it anymore
$node_publisher->safe_psql('postgres', "DROP PUBLICATION tap_pub_schema");

# Clean up the schemas on both publisher and subscriber as we don't need them
$node_publisher->safe_psql('postgres', "DROP SCHEMA sch1 cascade");
$node_publisher->safe_psql('postgres', "DROP SCHEMA sch2 cascade");
$node_publisher->safe_psql('postgres', "DROP SCHEMA sch3 cascade");
$node_subscriber->safe_psql('postgres', "DROP SCHEMA sch1 cascade");
$node_subscriber->safe_psql('postgres', "DROP SCHEMA sch2 cascade");
$node_subscriber->safe_psql('postgres', "DROP SCHEMA sch3 cascade");
