
# Copyright (c) 2024, PostgreSQL Global Development Group

# Test suite for testing enabling data checksums offline from various states
# of checksum processing
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

# Ensure that checksums are disabled
my $result = $node->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);
is($result, 'off', 'ensure checksums are disabled');

# Enable checksums offline using pg_checksums
$node->stop;
$node->checksum_enable_offline;
$node->start;

# Ensure that checksums are enabled
$result = $node->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);
is($result, 'on', 'ensure checksums are enabled');

# Run a dummy query just to make sure we can read back some data
$result = $node->safe_psql('postgres', "SELECT count(*) FROM t");
is($result, '10000', 'ensure checksummed pages can be read back');

# Disable checksums offline again using pg_checksums
$node->stop;
$node->checksum_disable_offline;
$node->start;

# Ensure that checksums are disabled
$result = $node->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);
is($result, 'off', 'ensure checksums are disabled');

# Create a barrier for checksumming to block on, in this case a pre-existing
# temporary table which is kept open while processing is started. We can
# accomplish this by setting up an interactive psql process which keeps the
# temporary table created as we enable checksums in another psql process.

my $bsession = $node->background_psql('postgres');
$bsession->query_safe('CREATE TEMPORARY TABLE tt (a integer);');

# In another session, make sure we can see the blocking temp table but start
# processing anyways and check that we are blocked with a proper wait event.
$result = $node->safe_psql('postgres',
	"SELECT relpersistence FROM pg_catalog.pg_class WHERE relname = 'tt';");
is($result, 't', 'ensure we can see the temporary table');

$node->safe_psql('postgres',
	"SELECT pg_enable_data_checksums($enable_params);");

$result = $node->poll_query_until(
	'postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'inprogress-on');
is($result, 1, 'ensure checksums are in the process of being enabled');

# Turn the cluster off and enable checksums offline, then start back up
$bsession->quit;
$node->stop;
$node->checksum_enable_offline;
$node->start;

# Ensure that checksums are now enabled even though processing wasn't
# restarted
$result = $node->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);
is($result, 'on', 'ensure checksums are enabled');

# Run a dummy query just to make sure we can read back some data
$result = $node->safe_psql('postgres', "SELECT count(*) FROM t");
is($result, '10000', 'ensure checksummed pages can be read back');

done_testing();
