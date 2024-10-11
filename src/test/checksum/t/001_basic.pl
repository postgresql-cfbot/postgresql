
# Copyright (c) 2024, PostgreSQL Global Development Group

# Test suite for testing enabling data checksums in an online cluster
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

# Initialize node with checksums disabled.
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init();
$node->start();

# Create some content to have un-checksummed data in the cluster
$node->safe_psql('postgres',
	"CREATE TABLE t AS SELECT generate_series(1,10000) AS a;");

# Ensure that checksums are turned off
my $result = $node->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);
is($result, 'off', 'ensure checksums are disabled');

# Enable data checksums
$node->safe_psql('postgres',
	"SELECT pg_enable_data_checksums($enable_params);");

# Wait for checksums to become enabled
$result = $node->poll_query_until(
	'postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'on');
is($result, 1, 'ensure checksums are enabled');

# Run a dummy query just to make sure we can read back some data
$result = $node->safe_psql('postgres', "SELECT count(*) FROM t");
is($result, '10000', 'ensure checksummed pages can be read back');

# Enable data checksums again which should be a no-op..
$node->safe_psql('postgres',
	"SELECT pg_enable_data_checksums($enable_params);");
# ..and make sure we can still read/write data
$node->safe_psql('postgres', "UPDATE t SET a = a + 1;");
$result = $node->safe_psql('postgres', "SELECT count(*) FROM t");
is($result, '10000', 'ensure checksummed pages can be read back');

# Disable checksums again
$node->safe_psql('postgres', "SELECT pg_disable_data_checksums();");

$result = $node->poll_query_until(
	'postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'off');
is($result, 1, 'ensure checksums are disabled');

# Test reading again
$result = $node->safe_psql('postgres', "SELECT count(*) FROM t");
is($result, '10000', 'ensure previously checksummed pages can be read back');

# Re-enable checksums and make sure that the underlying data has changed to
# ensure that checksums will be different.
$node->safe_psql('postgres', "UPDATE t SET a = a + 1;");

$node->safe_psql('postgres',
	"SELECT pg_enable_data_checksums($enable_params);");
$result = $node->poll_query_until(
	'postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'on');
is($result, 1, 'ensure checksums are enabled');

# Run a dummy query just to make sure we can read back the data
$result = $node->safe_psql('postgres', "SELECT count(*) FROM t");
is($result, '10000', 'ensure checksummed pages can be read back');

$node->stop;

done_testing();
