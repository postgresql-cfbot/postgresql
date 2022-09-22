
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Logical replication tests for except table publications
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# Test replication with publications created using FOR ALL TABLES EXCEPT TABLE
# clause.
# Create schemas and tables on publisher
$node_publisher->safe_psql('postgres', "CREATE SCHEMA sch1");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE sch1.tab1 AS SELECT generate_series(1,10) AS a");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE public.tab1(a int)");

# Create schemas and tables on subscriber
$node_subscriber->safe_psql('postgres', "CREATE SCHEMA sch1");
$node_subscriber->safe_psql('postgres', "CREATE TABLE sch1.tab1 (a int)");
$node_subscriber->safe_psql('postgres', "CREATE TABLE public.tab1 (a int)");

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_schema FOR ALL TABLES EXCEPT TABLE sch1.tab1");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub_schema CONNECTION '$publisher_connstr' PUBLICATION tap_pub_schema"
);

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub_schema');

# Check the table data does not sync for excluded table
my $result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM sch1.tab1");
is($result, qq(0||), 'check there is no initial data copied for the excluded table');

# Insert some data and verify that inserted data is not replicated
$node_publisher->safe_psql('postgres',
	"INSERT INTO sch1.tab1 VALUES(generate_series(11,20))");

$node_publisher->wait_for_catchup('tap_sub_schema');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM sch1.tab1");
is($result, qq(0||), 'check replicated inserts on subscriber');

# Alter publication to exclude data changes in public.tab1 and verify that
# subscriber does not get the changed data for this table.
$node_publisher->safe_psql('postgres',
        "ALTER PUBLICATION tap_pub_schema RESET");
$node_publisher->safe_psql('postgres',
        "ALTER PUBLICATION tap_pub_schema ADD ALL TABLES EXCEPT TABLE sch1.tab1, public.tab1");
$node_publisher->safe_psql('postgres',
        "INSERT INTO public.tab1 VALUES(generate_series(1,10))");

$node_publisher->wait_for_catchup('tap_sub_schema');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM public.tab1");
is($result, qq(0||), 'check rows on subscriber catchup');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');

done_testing();
