
# Copyright (c) 2024, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Test timeouts that will FATAL-out.
# This test relies on an injection points to await timeout ocurance.
# Relying on sleep prooved to be unstable on buildfarm.
# It's difficult to rely on NOTICE injection point, because FATALed
# backend can look differently under different circumstances.

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Initialize primary node
my $node = PostgreSQL::Test::Cluster->new('master');
$node->init();
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

$node->safe_psql('postgres',
	"SELECT injection_points_attach('TransactionTimeout', 'wait');");

my $psql_session =
  $node->background_psql('postgres', on_error_stop => 0);
$psql_session->query_until(
	qr/starting_bg_psql/, q(
   \echo starting_bg_psql
   SET transaction_timeout to '1ms';
   BEGIN;
   $$
));

# Wait until the backend is in the timeout.
ok( $node->poll_query_until(
		'postgres',
		qq[SELECT count(*) FROM pg_stat_activity
           WHERE wait_event = 'TransactionTimeout' ;],
		'1'),
	'backend is waiting in transaction timeout'
) or die "Timed out while waiting for transaction timeout";

$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('TransactionTimeout');");
$psql_session->quit;

$node->safe_psql('postgres',
	"SELECT injection_points_attach('IdleInTransactionSessionTimeout', 'wait');");

$psql_session =
  $node->background_psql('postgres', on_error_stop => 0);
$psql_session->query_until(
	qr/starting_bg_psql/, q(
   \echo starting_bg_psql
   SET idle_in_transaction_session_timeout to '10ms';
   BEGIN;
));

# Wait until the backend is in the timeout.
ok( $node->poll_query_until(
		'postgres',
		qq[SELECT count(*) FROM pg_stat_activity
           WHERE wait_event = 'IdleInTransactionSessionTimeout' ;],
		'1'),
	'backend is waiting in idle in transaction session timeout'
) or die "Timed out while waiting for idleness timeout";

$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('IdleInTransactionSessionTimeout');");
$psql_session->quit;

$node->safe_psql('postgres',
	"SELECT injection_points_attach('IdleSessionTimeout', 'wait');");

$psql_session =
  $node->background_psql('postgres', on_error_stop => 0);
$psql_session->query_until(
	qr/starting_bg_psql/, q(
   \echo starting_bg_psql
   SET idle_session_timeout to '10ms';
));

# Wait until the backend is in the timeout.
ok( $node->poll_query_until(
		'postgres',
		qq[SELECT count(*) FROM pg_stat_activity
           WHERE wait_event = 'IdleSessionTimeout' ;],
		'1'),
	'backend is waiting in idle session timeout'
) or die "Timed out while waiting for idleness timeout";

$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('IdleSessionTimeout');");
$psql_session->quit;


done_testing();
