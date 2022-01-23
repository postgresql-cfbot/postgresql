
# Copyright (c) 2021, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 2;

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 'logical');
$node_primary->append_conf('postgresql.conf', "standby_slot_names = 'pslot1'");
$node_primary->start;
$node_primary->psql('postgres', q{SELECT pg_create_physical_replication_slot('pslot1');});

$node_primary->backup('backup');

my $node_phys_standby = PostgreSQL::Test::Cluster->new('phys_standby');
$node_phys_standby->init_from_backup($node_primary, 'backup', has_streaming => 1);
$node_phys_standby->append_conf('postgresql.conf', "synchronize_slot_names = '*'");
$node_phys_standby->append_conf('postgresql.conf', "primary_slot_name = 'pslot1'");
$node_phys_standby->start;

$node_primary->safe_psql('postgres', "CREATE TABLE t1 (a int PRIMARY KEY)");
$node_primary->safe_psql('postgres', "INSERT INTO t1 VALUES (1), (2), (3)");

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

$node_subscriber->safe_psql('postgres', "CREATE TABLE t1 (a int PRIMARY KEY)");

$node_primary->safe_psql('postgres', "CREATE PUBLICATION pub1 FOR ALL TABLES");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub1 CONNECTION '" . ($node_primary->connstr . ' dbname=postgres') . "' PUBLICATION pub1");

# Wait for initial sync of all subscriptions
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

my $result = $node_primary->safe_psql('postgres',
	"SELECT slot_name, plugin, database FROM pg_replication_slots WHERE slot_type = 'logical'");

is($result, qq(sub1|pgoutput|postgres), 'logical slot on primary');

# FIXME: standby needs restart to pick up new slots
$node_phys_standby->restart;
sleep 3;

$result = $node_phys_standby->safe_psql('postgres',
	"SELECT slot_name, plugin, database FROM pg_replication_slots");

is($result, qq(sub1|pgoutput|postgres), 'logical slot on standby');

$node_primary->safe_psql('postgres', "INSERT INTO t1 VALUES (4), (5), (6)");
$node_primary->wait_for_catchup('sub1');
