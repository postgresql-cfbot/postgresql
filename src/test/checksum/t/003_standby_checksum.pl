# Test suite for testing enabling data checksums in an online cluster with
# streaming replication
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 10;

# Initialize primary node
my $node_primary = get_new_node('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->start;
my $backup_name = 'my_backup';

# Take backup
$node_primary->backup($backup_name);

# Create streaming standby linking to primary
my $node_standby_1 = get_new_node('standby_1');
$node_standby_1->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby_1->start;

# Create some content on primary to have un-checksummed data in the cluster
$node_primary->safe_psql('postgres',
	"CREATE TABLE t AS SELECT generate_series(1,10000) AS a;");

# Wait for standbys to catch up
$node_primary->wait_for_catchup($node_standby_1, 'replay',
	$node_primary->lsn('insert'));

# Check that checksums are turned off
my $result = $node_primary->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';");
is($result, "off", 'ensure checksums are turned off on primary');

$result = $node_standby_1->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';");
is($result, "off", 'ensure checksums are turned off on standby_1');

# Enable checksums for the cluster
$node_primary->safe_psql('postgres', "SELECT pg_enable_data_checksums();");

# Ensure that the primary switches to inprogress
$result = $node_primary->poll_query_until('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	"inprogress");
is($result, 1, 'ensure checksums are in progress on primary');

# Wait for checksum enable to be replayed
$node_primary->wait_for_catchup($node_standby_1, 'replay');

# Ensure that the standby has switched to inprogress or on
# Normally it would be "inprogress", but it is theoretically possible for the primary
# to complete the checksum enabling *and* have the standby replay that record before
# we reach the check below.
$result = $node_standby_1->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';");
cmp_ok($result, '~~', ["inprogress", "on"], 'ensure checksums are on or in progress on standby_1');

# Insert some more data which should be checksummed on INSERT
$node_primary->safe_psql('postgres',
	"INSERT INTO t VALUES (generate_series(1,10000));");

# Wait for checksums enabled on the primary
$result = $node_primary->poll_query_until('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'on');
is($result, 1, 'ensure checksums are enabled on the primary');

# Wait for checksums enabled on the standby
$result = $node_standby_1->poll_query_until('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'on');
is($result, 1, 'ensure checksums are enabled on the standby');

$result = $node_primary->safe_psql('postgres', "SELECT count(a) FROM t");
is ($result, '20000', 'ensure we can safely read all data with checksums');

# Disable checksums and ensure it's propagated to standby and that we can
# still read all data
$node_primary->safe_psql('postgres', "SELECT pg_disable_data_checksums();");
$result = $node_primary->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';");
is($result, 'off', 'ensure data checksums are disabled on the primary');

# Wait for checksum disable to be replayed
$node_primary->wait_for_catchup($node_standby_1, 'replay');

# Ensure that the standby has switched to off
$result = $node_standby_1->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';");
is($result, "off", 'ensure checksums are off on standby_1');

$result = $node_primary->safe_psql('postgres', "SELECT count(a) FROM t");
is ($result, "20000", 'ensure we can safely read all data without checksums');
