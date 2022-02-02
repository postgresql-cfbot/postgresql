
# Copyright (c) 2021, PostgreSQL Global Development Group

# Test of logical replication subscription self-disabling feature
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 8;

# Wait for the named subscriptions to catch up or to be disabled.
sub wait_for_subscriptions
{
	my ($node_name, $dbname, @subscriptions) = @_;

	# Unique-ify the subscriptions passed by the caller
	my %unique       = map { $_ => 1 } @subscriptions;
	my @unique       = sort keys %unique;
	my $unique_count = scalar(@unique);

	# Construct a SQL list from the unique subscription names
	my $sublist = join(', ', map { "'$_'" } @unique);

	my $polling_sql = qq(
		SELECT COUNT(1) = $unique_count FROM
			(SELECT s.oid
				FROM pg_catalog.pg_subscription s
				LEFT JOIN pg_catalog.pg_subscription_rel sr
				ON sr.srsubid = s.oid
				WHERE sr.srsubstate IN ('s', 'r')
				  AND s.subname IN ($sublist)
				  AND s.subenabled IS TRUE
			 UNION
			 SELECT s.oid
				FROM pg_catalog.pg_subscription s
				WHERE s.subname IN ($sublist)
				  AND s.subenabled IS FALSE
			) AS synced_or_disabled
		);
	return $node_name->poll_query_until($dbname, $polling_sql);
}

my @schemas = qw(s1 s2);
my ($schema, $cmd);

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->start;

# Create identical schema, table and index on both the publisher and
# subscriber
for $schema (@schemas)
{
	$cmd = qq(
CREATE SCHEMA $schema;
CREATE TABLE $schema.tbl (i INT);
ALTER TABLE $schema.tbl REPLICA IDENTITY FULL;
CREATE INDEX ${schema}_tbl_idx ON $schema.tbl(i));
	$node_publisher->safe_psql('postgres', $cmd);
	$node_subscriber->safe_psql('postgres', $cmd);
}

# Create non-unique data in both schemas on the publisher.
for $schema (@schemas)
{
	$cmd = qq(INSERT INTO $schema.tbl (i) VALUES (1), (1), (1));
	$node_publisher->safe_psql('postgres', $cmd);
}

# Create an additional unique index in schema s1 on the subscriber only.  When
# we create subscriptions, below, this should cause subscription "s1" on the
# subscriber to fail during initial synchronization and to get automatically
# disabled.
$cmd = qq(CREATE UNIQUE INDEX s1_tbl_unique ON s1.tbl (i));
$node_subscriber->safe_psql('postgres', $cmd);

# Create publications and subscriptions linking the schemas on
# the publisher with those on the subscriber.  This tests that the
# uniqueness violations cause subscription "s1" to fail during
# initial synchronization.
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
for $schema (@schemas)
{
	# Create the publication for this table
	$cmd = qq(
CREATE PUBLICATION $schema FOR TABLE $schema.tbl);
	$node_publisher->safe_psql('postgres', $cmd);

	# Create the subscription for this table
	$cmd = qq(
CREATE SUBSCRIPTION $schema
	CONNECTION '$publisher_connstr'
	PUBLICATION $schema
	WITH (disable_on_error = true));
	$node_subscriber->safe_psql('postgres', $cmd);
}

# Wait for the initial subscription synchronizations to finish or fail.
wait_for_subscriptions($node_subscriber, 'postgres', @schemas)
  or die "Timed out while waiting for subscriber to synchronize data";

# Subscription "s1" should have disabled itself due to error.
$cmd = qq(
SELECT subenabled FROM pg_catalog.pg_subscription WHERE subname = 's1');
is($node_subscriber->safe_psql('postgres', $cmd),
	"f", "subscription s1 no longer enabled");

# Subscription "s2" should have copied the initial data without incident.
$cmd = qq(
SELECT subenabled FROM pg_catalog.pg_subscription WHERE subname = 's2');
is($node_subscriber->safe_psql('postgres', $cmd),
	"t", "subscription s2 still enabled");
$cmd = qq(SELECT i, COUNT(*) FROM s2.tbl GROUP BY i);
is($node_subscriber->safe_psql('postgres', $cmd),
	"1|3", "subscription s2 replicated initial data");

# Enter unique data for both schemas on the publisher.  This should succeed on
# the publisher node, and not cause any additional problems on the subscriber
# side either, though disabled subscription "s1" should not replicate anything.
for $schema (@schemas)
{
	$cmd = qq(INSERT INTO $schema.tbl (i) VALUES (2));
	$node_publisher->safe_psql('postgres', $cmd);
}

# Wait for the data to replicate for the subscriptions.  This tests that the
# problems encountered by subscription "s1" do not cause subscription "s2" to
# get stuck. Subscription "s1" should still be disabled.
$cmd = qq(
SELECT subenabled FROM pg_catalog.pg_subscription WHERE subname = 's1');
is($node_subscriber->safe_psql('postgres', $cmd),
	"f", "subscription s1 still disabled");

# Subscription "s2" should still be enabled and have replicated all changes
$cmd = qq(
SELECT subenabled FROM pg_catalog.pg_subscription WHERE subname = 's2');
is($node_subscriber->safe_psql('postgres', $cmd),
	"t", "subscription s2 still enabled");
$cmd = q(SELECT COUNT(1) = 1 FROM s2.tbl WHERE i = 2);
$node_subscriber->poll_query_until('postgres', $cmd)
  or die "Timed out while waiting for subscriber to apply data";

# Drop the unique index on "s1" which caused the subscription to be disabled
$cmd = qq(DROP INDEX s1.s1_tbl_unique);
$node_subscriber->safe_psql('postgres', $cmd);

# Re-enable the subscription "s1"
$cmd = q(ALTER SUBSCRIPTION s1 ENABLE);
$node_subscriber->safe_psql('postgres', $cmd);

# Wait for the data to replicate
wait_for_subscriptions($node_subscriber, 'postgres', @schemas)
  or die "Timed out while waiting for subscriber to synchronize data";

# Check that we have the new data in s1.tbl
$cmd = q(SELECT MAX(i), COUNT(*) FROM s1.tbl);
is($node_subscriber->safe_psql('postgres', $cmd),
	"2|4", "subscription s1 replicated data");

# Delete the data from the subscriber only, and recreate the unique index
$cmd = q(
DELETE FROM s1.tbl;
CREATE UNIQUE INDEX s1_tbl_unique ON s1.tbl (i));
$node_subscriber->safe_psql('postgres', $cmd);

# Add more non-unique data to the publisher
for $schema (@schemas)
{
	$cmd = qq(INSERT INTO $schema.tbl (i) VALUES (3), (3), (3));
	$node_publisher->safe_psql('postgres', $cmd);
}

# Wait for the data to replicate for the subscriptions.  This tests that
# uniqueness violations encountered during replication cause s1 to be disabled.
$cmd = qq(
SELECT count(1) = 1 FROM pg_catalog.pg_subscription s
WHERE s.subname = 's1' AND s.subenabled IS FALSE
);
$node_subscriber->poll_query_until('postgres', $cmd)
  or die "Timed out while waiting for subscription s1 to be disabled";

# Subscription "s2" should have copied the initial data without incident.
$cmd = qq(
SELECT subenabled FROM pg_catalog.pg_subscription WHERE subname = 's2');
is($node_subscriber->safe_psql('postgres', $cmd),
	"t", "subscription s2 still enabled");
$cmd = qq(SELECT MAX(i), COUNT(*) FROM s2.tbl);
is($node_subscriber->safe_psql('postgres', $cmd),
	"3|7", "subscription s2 replicated additional data");

$node_subscriber->stop;
$node_publisher->stop;
