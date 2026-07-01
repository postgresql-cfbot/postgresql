# Copyright (c) 2025-2026, PostgreSQL Global Development Group
#
# Verify that walreceiver transitions to WALRCV_SWITCHING_TIMELINE state
# while fetching the timeline history file after detecting an end-of-timeline
# from the primary.  During that window the startup process must not kill
# walreceiver; instead it should back off and sleep.
#
# Topology:
#   node_primary (TL1) --> node_promotable (will promote to TL2)
#                      --> node_test (cascaded from node_promotable;
#                                    walreceiver here detects the TL switch)
#
# When node_promotable promotes (TL1->TL2), its walsender sends an
# end-of-timeline notification to node_test's walreceiver.  Walreceiver
# transitions to WALRCV_SWITCHING_TIMELINE and pauses at the injection point,
# giving us a stable window to observe both walreceiver and startup.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Primary node on TL1.
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->start;

if (!$node_primary->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node_primary->backup('backup');

# node_promotable: streaming standby that will be promoted to TL2.
my $node_promotable = PostgreSQL::Test::Cluster->new('promotable');
$node_promotable->init_from_backup($node_primary, 'backup',
	has_streaming => 1);
$node_promotable->start;
$node_primary->wait_for_catchup($node_promotable);

# node_test: cascaded streaming standby of node_promotable.
# Its walreceiver will detect the TL switch when node_promotable promotes.
my $node_test = PostgreSQL::Test::Cluster->new('test');
$node_test->init_from_backup($node_primary, 'backup', has_streaming => 1);

# Override primary_conninfo to stream from node_promotable, not node_primary.
$node_test->append_conf('postgresql.conf',
	"primary_conninfo = '" . $node_promotable->connstr . "'");

# Use a short retry interval so startup quickly enters
# RecoveryRetrieveRetryInterval once walreceiver stops "streaming".
$node_test->append_conf('postgresql.conf',
	'wal_retrieve_retry_interval = 500ms');
$node_test->start;

# Wait for node_test to catch up with node_promotable via cascade.
$node_promotable->wait_for_catchup($node_test);

# Create the injection_points extension on the primary and let it replicate
# through node_promotable to node_test.
$node_primary->safe_psql('postgres', 'CREATE EXTENSION injection_points;');
$node_primary->safe_psql('postgres', 'CREATE TABLE t (a int);');
$node_primary->wait_for_catchup($node_promotable);
$node_promotable->wait_for_catchup($node_test);

# Attach the injection point on node_test.  Walreceiver will pause here
# immediately after transitioning to WALRCV_SWITCHING_TIMELINE, before
# calling WalRcvFetchTimeLineHistoryFiles().
$node_test->safe_psql('postgres',
	"SELECT injection_points_attach('walreceiver-switch-timeline', 'wait');");

# Promote node_promotable: it becomes the TL2 primary and its walsender
# sends an end-of-timeline notification to node_test's walreceiver.
$node_promotable->promote;
$node_promotable->poll_query_until('postgres',
	'SELECT NOT pg_is_in_recovery()')
  or die 'Timed out waiting for node_promotable to promote to TL2';

$node_promotable->safe_psql('postgres', 'INSERT INTO t VALUES (1);');

# Wait until node_test's walreceiver has reached the injection point.
# Reaching it proves that walreceiver entered WALRCV_SWITCHING_TIMELINE and
# was not killed by the startup process before it could fetch the history file.
$node_test->wait_for_event('walreceiver', 'walreceiver-switch-timeline');
ok(1, 'walreceiver paused in WALRCV_SWITCHING_TIMELINE state');

# With WALRCV_SWITCHING_TIMELINE, WalRcvStreaming() returns false, so the
# startup process does not call XLogShutdownWalRcv().  Instead it backs off
# and sleeps in wal_retrieve_retry_interval.  Verify that here.
$node_test->wait_for_event('startup', 'RecoveryRetrieveRetryInterval');
ok(1,
	'startup sleeping in RecoveryRetrieveRetryInterval, not killing walreceiver'
);

# Release walreceiver so it can fetch the TL2 timeline history file.
$node_test->safe_psql('postgres',
	"SELECT injection_points_detach('walreceiver-switch-timeline');
	SELECT injection_points_wakeup('walreceiver-switch-timeline');");

# Some data on TL2 to be observed on standby
$node_promotable->safe_psql('postgres', 'INSERT INTO t VALUES (1);');

# After the fetch, walreceiver transitions to WALRCV_WAITING, wakes startup,
# and startup restarts walreceiver on TL2.  node_test should then replay TL2.
$node_test->poll_query_until('postgres',
	'SELECT count(*) = 2 FROM t;')
  or die 'Timed out waiting for node_test to switch to TL2';
ok(1, 'standby successfully switched to TL2 after timeline history fetch');

done_testing();
