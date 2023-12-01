# Test consistent of initial snapshot data.

# This requires a node with wal_level=logical combined with an injection
# point that forces a failure when a snapshot is initially built with a
# logical slot created.
#
# See bug https://postgr.es/m/CAFiTN-s0zA1Kj0ozGHwkYkHwa5U0zUE94RSc_g81WrpcETB5=w@mail.gmail.com.

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init(allows_streaming => 'logical');
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION test_injection_points;');
$node->safe_psql('postgres',
  "SELECT test_injection_points_attach('SnapBuildInitialSnapshot', 'error');");

my $node_host = $node->host;
my $node_port = $node->port;
my $connstr_common = "host=$node_host port=$node_port";
my $connstr_db = "$connstr_common replication=database dbname=postgres";

# This requires a single session, with two commands.
my $psql_session =
  $node->background_psql('postgres', on_error_stop => 0,
			 extra_params => [ '-d', $connstr_db ]);
my ($output, $ret) = $psql_session->query(
    'CREATE_REPLICATION_SLOT "slot" LOGICAL "pgoutput";');
ok($ret != 0, "First CREATE_REPLICATION_SLOT fails on injected error");

# Now remove the injected error and check that the second command works.
$node->safe_psql('postgres',
  "SELECT test_injection_points_detach('SnapBuildInitialSnapshot');");

($output, $ret) = $psql_session->query(
    'CREATE_REPLICATION_SLOT "slot" LOGICAL "pgoutput";');
print "BOO" . substr($output, 0, 4) . "\n";
ok(substr($output, 0, 4) eq 'slot',
   "Second CREATE_REPLICATION_SLOT passes");

done_testing();
