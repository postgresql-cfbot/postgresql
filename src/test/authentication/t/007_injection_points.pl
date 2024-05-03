
# Copyright (c) 2021-2024, PostgreSQL Global Development Group

# Tests requiring injection_points functionality, to check on behavior that
# would otherwise race against authentication.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf(
	'postgresql.conf', q[
log_connections = on
]);

$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');

# Connect to the server and inject a waitpoint.
my $psql = $node->background_psql('postgres');
$psql->query_safe("SELECT injection_points_attach('init-pre-auth', 'wait')");

# From this point on, all new connections will hang in authentication. Use the
# $psql connection handle for server interaction.
my $conn = $node->background_psql('postgres', wait => 0);

# Wait for the connection to show up.
my $pid;
while (1)
{
	$pid = $psql->query(
		"SELECT pid FROM pg_stat_activity WHERE state = 'authenticating';");
	last if $pid ne "";

	usleep(500_000);
}

note "backend $pid is authenticating";
ok(1, 'authenticating connections are recorded in pg_stat_activity');

# Detach the waitpoint and wait for the connection to complete.
$psql->query_safe("SELECT injection_points_wakeup('init-pre-auth');");
$conn->wait_connect();

# Make sure the pgstat entry is updated eventually.
while (1)
{
	my $state = $psql->query(
		"SELECT state FROM pg_stat_activity WHERE pid = $pid;");
	last if $state eq "idle";

	note "state for backend $pid is '$state'; waiting for 'idle'...";
	usleep(500_000);
}

ok(1, 'authenticated connections reach idle state in pg_stat_activity');

$psql->query_safe("SELECT injection_points_detach('init-pre-auth');");
$psql->quit();
$conn->quit();

done_testing();
