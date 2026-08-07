# Copyright (c) 2025-2026, PostgreSQL Global Development Group

# Test that ALTER SUBSCRIPTION ... REFRESH PUBLICATION skips, rather than
# crashes on, a subscribed relation dropped concurrently during the refresh.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->start;

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

# The refresh runs on the subscriber, so arm the injection point there.
$node_subscriber->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

# tab_keep stays, tab_drop is dropped during the refresh.
$node_publisher->safe_psql(
	'postgres', q{
	CREATE TABLE tab_keep (a int);
	CREATE TABLE tab_drop (a int);
	CREATE PUBLICATION mypub FOR ALL TABLES;
});

$node_subscriber->safe_psql(
	'postgres', q{
	CREATE TABLE tab_keep (a int);
	CREATE TABLE tab_drop (a int);
});

# origin = none is the path that reads each local relation's name.
$node_subscriber->safe_psql(
	'postgres', qq{
	CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr'
		PUBLICATION mypub WITH (copy_data = false, origin = none);
});

$node_subscriber->wait_for_subscription_sync($node_publisher, 'mysub');

# Pause the next refresh after it collects the local relation list.
$node_subscriber->safe_psql('postgres',
	"SELECT injection_points_attach('subscription-refresh-before-origin-check', 'wait');"
);

# Run the refresh in the background. It blocks at the injection point.
my $bg = $node_subscriber->background_psql('postgres');
$bg->query_until(
	qr/starting_refresh/, q{
	\echo starting_refresh
	ALTER SUBSCRIPTION mysub REFRESH PUBLICATION;
});

$node_subscriber->wait_for_event('client backend',
	'subscription-refresh-before-origin-check');

# Drop the table while the refresh holds its now-stale OID.
$node_subscriber->safe_psql('postgres', 'DROP TABLE tab_drop;');

# Wake the refresh. It crashed here before the fix.
$node_subscriber->safe_psql('postgres',
	"SELECT injection_points_wakeup('subscription-refresh-before-origin-check');"
);

$bg->quit;

# A successful query proves the backend survived.
is($node_subscriber->safe_psql('postgres', 'SELECT 1;'),
	'1', 'subscriber survived refresh with concurrently dropped table');

$node_subscriber->safe_psql('postgres',
	"SELECT injection_points_detach('subscription-refresh-before-origin-check');"
);

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
