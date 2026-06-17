
# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Tests for connection behavior prior to authentication.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Session;
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
log_connections = 'receipt,authentication'
]);

$node->start;

# Check if the extension injection_points is available, as it may be
# possible that this script is run with installcheck, where the module
# would not be installed by default.
if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');

# Connect to the server and inject a waitpoint.
my $session = PostgreSQL::Test::Session->new(node => $node);
$session->do("SELECT injection_points_attach('init-pre-auth', 'wait')");

# From this point on, all new connections will hang during startup, just before
# authentication. Use the $session connection handle for server interaction.
my $conn = PostgreSQL::Test::Session->new(node => $node, wait => 0);

# Wait for the connection to show up in pg_stat_activity, with the wait_event
# of the injection point. We need to poll the async connection to drive it forward.
my $pid;
while (1)
{
	# Drive the async connection forward - it won't progress without polling
	$conn->poll_connect();

	$pid = $session->query_oneval(
		qq{SELECT pid FROM pg_stat_activity
  WHERE backend_type = 'client backend'
    AND state = 'starting'
    AND wait_event = 'init-pre-auth';}, 1);
	last if defined $pid && $pid ne "";

	usleep(100_000);
}

note "backend $pid is authenticating";
ok(1, 'authenticating connections are recorded in pg_stat_activity');

# Detach the waitpoint and wait for the connection to complete.
$session->do("SELECT injection_points_wakeup('init-pre-auth')");
$conn->wait_connect();

# Make sure the pgstat entry is updated eventually.
while (1)
{
	my $state =
	  $session->query_oneval(
		"SELECT state FROM pg_stat_activity WHERE pid = $pid", 1);
	last if defined $state && $state eq "idle";

	note "state for backend $pid is '" . ($state // 'undef') . "'; waiting for 'idle'...";
	usleep(100_000);
}

ok(1, 'authenticated connections reach idle state in pg_stat_activity');

$session->do("SELECT injection_points_detach('init-pre-auth')");
$session->close();
$conn->close();

done_testing();
