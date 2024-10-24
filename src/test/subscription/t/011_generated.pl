
# Copyright (c) 2021-2024, PostgreSQL Global Development Group

# Test generated columns
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# setup

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->start;

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab1 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2) STORED)"
);

$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab1 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 22) STORED, c int)"
);

# data for initial sync

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab1 (a) VALUES (1), (2), (3)");

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub1 FOR ALL TABLES");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub1 CONNECTION '$publisher_connstr' PUBLICATION pub1"
);

# Wait for initial sync of all subscriptions
$node_subscriber->wait_for_subscription_sync;

my $result = $node_subscriber->safe_psql('postgres', "SELECT a, b FROM tab1");
is( $result, qq(1|22
2|44
3|66), 'generated columns initial sync');

# data to replicate

$node_publisher->safe_psql('postgres', "INSERT INTO tab1 VALUES (4), (5)");

$node_publisher->safe_psql('postgres', "UPDATE tab1 SET a = 6 WHERE a = 5");

$node_publisher->wait_for_catchup('sub1');

$result = $node_subscriber->safe_psql('postgres', "SELECT * FROM tab1");
is( $result, qq(1|22|
2|44|
3|66|
4|88|
6|132|), 'generated columns replicated');

# try it with a subscriber-side trigger

$node_subscriber->safe_psql(
	'postgres', q{
CREATE FUNCTION tab1_trigger_func() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.c := NEW.a + 10;
  RETURN NEW;
END $$;

CREATE TRIGGER test1 BEFORE INSERT OR UPDATE ON tab1
  FOR EACH ROW
  EXECUTE PROCEDURE tab1_trigger_func();

ALTER TABLE tab1 ENABLE REPLICA TRIGGER test1;
});

$node_publisher->safe_psql('postgres', "INSERT INTO tab1 VALUES (7), (8)");

$node_publisher->safe_psql('postgres', "UPDATE tab1 SET a = 9 WHERE a = 7");

$node_publisher->wait_for_catchup('sub1');

$result =
  $node_subscriber->safe_psql('postgres', "SELECT * FROM tab1 ORDER BY 1");
is( $result, qq(1|22|
2|44|
3|66|
4|88|
6|132|
8|176|18
9|198|19), 'generated columns replicated with trigger');

# cleanup
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION sub1");
$node_publisher->safe_psql('postgres', "DROP PUBLICATION pub1");

# =============================================================================
# The following test cases exercise logical replication for the combinations
# where there is a generated column on one or both sides of pub/sub:
# - generated -> normal
#
# Furthermore, the combinations are tested using:
# a publication pub1, on the 'postgres' database, with option publish_generated_columns=false.
# a publication pub2, on the 'postgres' database, with option publish_generated_columns=true.
# a subscription sub1, on the 'postgres' database for publication pub1.
# a subscription sub2, on the 'test_pgc_true' database for publication pub2.
# =============================================================================

$node_subscriber->safe_psql('postgres', "CREATE DATABASE test_pgc_true");

# --------------------------------------------------
# Testcase: generated -> normal
# Publisher table has generated column 'b'.
# Subscriber table has normal column 'b'.
# --------------------------------------------------

# Create table and publications.
$node_publisher->safe_psql(
	'postgres', qq(
	CREATE TABLE tab_gen_to_nogen (a int, b int GENERATED ALWAYS AS (a * 2) STORED);
	INSERT INTO tab_gen_to_nogen (a) VALUES (1), (2), (3);
	CREATE PUBLICATION regress_pub1_gen_to_nogen FOR TABLE tab_gen_to_nogen WITH (publish_generated_columns = false);
	CREATE PUBLICATION regress_pub2_gen_to_nogen FOR TABLE tab_gen_to_nogen WITH (publish_generated_columns = true);
));

# Create table and subscription with copy_data=true.
$node_subscriber->safe_psql(
	'postgres', qq(
	CREATE TABLE tab_gen_to_nogen (a int, b int);
	CREATE SUBSCRIPTION regress_sub1_gen_to_nogen CONNECTION '$publisher_connstr' PUBLICATION regress_pub1_gen_to_nogen WITH (copy_data = true);
));

# Create table and subscription.
$node_subscriber->safe_psql(
	'test_pgc_true', qq(
	CREATE TABLE tab_gen_to_nogen (a int, b int);
	CREATE SUBSCRIPTION regress_sub2_gen_to_nogen CONNECTION '$publisher_connstr' PUBLICATION regress_pub2_gen_to_nogen WITH (copy_data = true);
));

# Wait for initial sync.
$node_subscriber->wait_for_subscription_sync($node_publisher,
	'regress_sub1_gen_to_nogen', 'postgres');
$node_subscriber->wait_for_subscription_sync($node_publisher,
	'regress_sub2_gen_to_nogen', 'test_pgc_true');

# Initial sync test when publish_generated_columns=false.
# Verify that column 'b' is not replicated.
$result = $node_subscriber->safe_psql('postgres',
	"SELECT a, b FROM tab_gen_to_nogen");
is( $result, qq(1|
2|
3|), 'tab_gen_to_nogen initial sync, when publish_generated_columns=false');

# Initial sync test when publish_generated_columns=true.
$result = $node_subscriber->safe_psql('test_pgc_true',
	"SELECT a, b FROM tab_gen_to_nogen");
is( $result, qq(1|2
2|4
3|6),
	'tab_gen_to_nogen initial sync, when publish_generated_columns=true');

# Insert data to verify incremental replication
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_gen_to_nogen VALUES (4), (5)");

# Incremental replication test when publish_generated_columns=false.
# Verify that column 'b' is not replicated.
$node_publisher->wait_for_catchup('regress_sub1_gen_to_nogen');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT a, b FROM tab_gen_to_nogen ORDER BY a");
is( $result, qq(1|
2|
3|
4|
5|),
	'tab_gen_to_nogen incremental replication, when publish_generated_columns=false'
);

# Incremental replication test when publish_generated_columns=true.
# Verify that column 'b' is replicated.
$node_publisher->wait_for_catchup('regress_sub2_gen_to_nogen');
$result = $node_subscriber->safe_psql('test_pgc_true',
	"SELECT a, b FROM tab_gen_to_nogen ORDER BY a");
is( $result, qq(1|2
2|4
3|6
4|8
5|10),
	'tab_gen_to_nogen incremental replication, when publish_generated_columns=true'
);

# cleanup
$node_subscriber->safe_psql('postgres',
	"DROP SUBSCRIPTION regress_sub1_gen_to_nogen");
$node_subscriber->safe_psql('test_pgc_true',
	"DROP SUBSCRIPTION regress_sub2_gen_to_nogen");
$node_publisher->safe_psql(
	'postgres', qq(
	DROP PUBLICATION regress_pub1_gen_to_nogen;
	DROP PUBLICATION regress_pub2_gen_to_nogen;
));
$node_subscriber->safe_psql('test_pgc_true', "DROP table tab_gen_to_nogen");
$node_subscriber->safe_psql('postgres', "DROP DATABASE test_pgc_true");

# =============================================================================
# The following test cases demonstrate behavior of generated column replication
# when publish_generated_colums=false/true:
#
# Test: column list includes gencols, when publish_generated_columns=false
# Test: column list does not include gencols, when publish_generated_columns=false
#
# Test: column list includes gencols, when publish_generated_columns=true
# Test: column list does not include gencols, when publish_generated_columns=true
# =============================================================================

# --------------------------------------------------
# Testcase: Publisher replicates the column list data including generated
# columns even though publish_generated_columns option is false.
# --------------------------------------------------

# Create table and publications.
$node_publisher->safe_psql(
	'postgres', qq(
	CREATE TABLE tab_gen_to_gen (a int, gen1 int GENERATED ALWAYS AS (a * 2) STORED);
	CREATE TABLE tab_gen_to_gen2 (a int, gen1 int GENERATED ALWAYS AS (a * 2) STORED);
	CREATE PUBLICATION pub1 FOR table tab_gen_to_gen, tab_gen_to_gen2(gen1) WITH (publish_generated_columns=false);
));

# Insert values into tables.
$node_publisher->safe_psql(
	'postgres', qq(
	INSERT INTO tab_gen_to_gen (a) VALUES (1), (1);
	INSERT INTO tab_gen_to_gen2 (a) VALUES (1), (1);
));

# Create table and subscription with copy_data=true.
$node_subscriber->safe_psql(
	'postgres', qq(
	CREATE TABLE tab_gen_to_gen (a int, gen1 int);
	CREATE TABLE tab_gen_to_gen2 (a int, gen1 int);
	CREATE SUBSCRIPTION sub1 CONNECTION '$publisher_connstr' PUBLICATION pub1 WITH (copy_data = true);
));

# Wait for initial sync.
$node_subscriber->wait_for_subscription_sync;
$node_publisher->wait_for_catchup('sub1');

# Initial sync test when publish_generated_columns=false.
$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab_gen_to_gen ORDER BY a");
is( $result, qq(1|
1|),
	'tab_gen_to_gen initial sync, when publish_generated_columns=false');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab_gen_to_gen2 ORDER BY a");
is( $result, qq(|2
|2),
	'tab_gen_to_gen2 initial sync, when publish_generated_columns=false');

# Insert data to verify incremental replication
$node_publisher->safe_psql(
	'postgres', qq(
	INSERT INTO tab_gen_to_gen VALUES (2), (3);
	INSERT INTO tab_gen_to_gen2 VALUES (2), (3);
));

# Incremental replication test when publish_generated_columns=false.
# Verify that column 'b' is not replicated.
$node_publisher->wait_for_catchup('sub1');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab_gen_to_gen ORDER BY a");
is( $result, qq(1|
1|
2|
3|),
	'tab_gen_to_gen incremental replication, when publish_generated_columns=false'
);
$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab_gen_to_gen2 ORDER BY a");
is( $result, qq(|2
|2
|4
|6),
	'tab_gen_to_gen2 incremental replication, when publish_generated_columns=false'
);

# cleanup
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION sub1");
$node_publisher->safe_psql('postgres', "DROP PUBLICATION pub1");

# --------------------------------------------------
# Testcase: Although publish_generated_columns is true, publisher publishes
# only the data of the columns specified in column list, skipping other
# generated/non-generated columns.
# --------------------------------------------------

# Create table and publications.
$node_publisher->safe_psql(
	'postgres', qq(
	CREATE TABLE tab_gen_to_gen3 (a int, gen1 int GENERATED ALWAYS AS (a * 2) STORED);
	CREATE TABLE tab_gen_to_gen4 (a int, gen1 int GENERATED ALWAYS AS (a * 2) STORED);
	CREATE PUBLICATION pub1 FOR table tab_gen_to_gen3, tab_gen_to_gen4(gen1) WITH (publish_generated_columns=true);
));

# Insert values into tables.
$node_publisher->safe_psql(
	'postgres', qq(
	INSERT INTO tab_gen_to_gen3 (a) VALUES (1), (1);
	INSERT INTO tab_gen_to_gen4 (a) VALUES (1), (1);
));

# Create table and subscription with copy_data=true.
$node_subscriber->safe_psql(
	'postgres', qq(
	CREATE TABLE tab_gen_to_gen3 (a int, gen1 int);
	CREATE TABLE tab_gen_to_gen4 (a int, gen1 int);
	CREATE SUBSCRIPTION sub1 CONNECTION '$publisher_connstr' PUBLICATION pub1 WITH (copy_data = true);
));

# Wait for initial sync.
$node_subscriber->wait_for_subscription_sync;
$node_publisher->wait_for_catchup('sub1');

# Initial sync test when publish_generated_columns=true.
$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab_gen_to_gen3 ORDER BY a");
is( $result, qq(1|2
1|2),
	'tab_gen_to_gen3 initial sync, when publish_generated_columns=true');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab_gen_to_gen4 ORDER BY a");
is( $result, qq(|2
|2),
	'tab_gen_to_gen4 initial sync, when publish_generated_columns=true');

# Insert data to verify incremental replication.
# Verify that column 'b' is replicated.
$node_publisher->safe_psql(
	'postgres', qq(
	INSERT INTO tab_gen_to_gen3 VALUES (2), (3);
	INSERT INTO tab_gen_to_gen4 VALUES (2), (3);
));

# Incremental replication test when publish_generated_columns=true.
# Verify that column 'b' is replicated.
$node_publisher->wait_for_catchup('sub1');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab_gen_to_gen3 ORDER BY a");
is( $result, qq(1|2
1|2
2|4
3|6),
	'tab_gen_to_gen3 incremental replication, when publish_generated_columns=true'
);
$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab_gen_to_gen4 ORDER BY a");
is( $result, qq(|2
|2
|4
|6),
	'tab_gen_to_gen4 incremental replication, when publish_generated_columns=true'
);

# cleanup
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION sub1");
$node_publisher->safe_psql('postgres', "DROP PUBLICATION pub1");

done_testing();
