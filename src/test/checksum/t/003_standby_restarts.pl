
# Copyright (c) 2024, PostgreSQL Global Development Group

# Test suite for testing enabling data checksums in an online cluster with
# streaming replication
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# pg_enable_checksums take three params: cost_delay, cost_limit and fast. For
# testing we always want to override the default value for 'fast' with True
# which will cause immediate checkpoints. 0 and 100 are the defaults for
# cost_delay and cost_limit which are fine to use for testing so let's keep
# them.
my $enable_params = '0, 100, true';

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->start;
my $backup_name = 'my_backup';

# Take backup
$node_primary->backup($backup_name);

# Create streaming standby linking to primary
my $node_standby_1 = PostgreSQL::Test::Cluster->new('standby_1');
$node_standby_1->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby_1->start;

# Create some content on the primary to have un-checksummed data in the cluster
$node_primary->safe_psql('postgres',
	"CREATE TABLE t AS SELECT generate_series(1,10000) AS a;");

# Wait for standbys to catch up
$node_primary->wait_for_catchup($node_standby_1, 'replay',
	$node_primary->lsn('insert'));

# Check that checksums are turned off on all nodes
my $result = $node_primary->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);
is($result, "off", 'ensure checksums are turned off on primary');

$result = $node_standby_1->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);
is($result, "off", 'ensure checksums are turned off on standby_1');

# Enable checksums for the cluster
$node_primary->safe_psql('postgres',
	"SELECT pg_enable_data_checksums($enable_params);");

# Ensure that the primary switches to "inprogress-on"
$result = $node_primary->poll_query_until(
	'postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	"inprogress-on");
is($result, 1, 'ensure checksums are in progress on primary');

# Wait for checksum enable to be replayed
$node_primary->wait_for_catchup($node_standby_1, 'replay');

# Ensure that the standby has switched to "inprogress-on" or "on".  Normally it
# would be "inprogress-on", but it is theoretically possible for the primary to
# complete the checksum enabling *and* have the standby replay that record
# before we reach the check below.
$result = $node_standby_1->poll_query_until(
	'postgres',
	"SELECT setting = 'off' FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'f');
is($result, 1, 'ensure standby has absorbed the inprogress-on barrier');
$result = $node_standby_1->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);

is(($result eq 'inprogress-on' || $result eq 'on'),
	1, 'ensure checksums are on, or in progress, on standby_1');

# Insert some more data which should be checksummed on INSERT
$node_primary->safe_psql('postgres',
	"INSERT INTO t VALUES (generate_series(1, 10000));");

# Wait for checksums enabled on the primary
$result = $node_primary->poll_query_until(
	'postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'on');
is($result, 1, 'ensure checksums are enabled on the primary');

# Wait for checksums enabled on the standby
$result = $node_standby_1->poll_query_until(
	'postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'on');
is($result, 1, 'ensure checksums are enabled on the standby');

$result = $node_primary->safe_psql('postgres', "SELECT count(a) FROM t");
is($result, '20000', 'ensure we can safely read all data with checksums');

$result = $node_primary->poll_query_until(
	'postgres',
	"SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'datachecksumsworker%';",
	'0');
is($result, 1, 'await datachecksums worker/launcher termination');

# Disable checksums and ensure it's propagated to standby and that we can
# still read all data
$node_primary->safe_psql('postgres', "SELECT pg_disable_data_checksums();");
# Wait for checksum disable to be replayed
$node_primary->wait_for_catchup($node_standby_1, 'replay');
$result = $node_primary->poll_query_until(
	'postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'off');
is($result, 1, 'ensure data checksums are disabled on the primary 2');

# Ensure that the standby has switched to off
$result = $node_standby_1->poll_query_until(
	'postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'off');
is($result, 1, 'ensure checksums are off on standby_1');

$result = $node_primary->safe_psql('postgres', "SELECT count(a) FROM t");
is($result, "20000", 'ensure we can safely read all data without checksums');

done_testing();
