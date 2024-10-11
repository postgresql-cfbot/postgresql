
# Copyright (c) 2024, PostgreSQL Global Development Group

# Test suite for testing enabling data checksums in an online cluster with
# restarting the processing
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
$node->init;
$node->start;

# Create some content to have un-checksummed data in the cluster
$node->safe_psql('postgres',
	"CREATE TABLE t AS SELECT generate_series(1,10000) AS a;");

# Ensure that checksums are disabled
my $result = $node->safe_psql('postgres',
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
is($result, '1', "ensure checksums aren't enabled yet");

$bsession->quit;
$node->stop;
$node->start;

$result = $node->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);
is($result, 'inprogress-on', "ensure checksums aren't enabled yet");

$node->safe_psql('postgres',
	"SELECT pg_enable_data_checksums($enable_params);");

$result = $node->poll_query_until(
	'postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'on');
is($result, 1, 'ensure checksums are turned on');

$result = $node->safe_psql('postgres', "SELECT count(*) FROM t");
is($result, '10000', 'ensure checksummed pages can be read back');

$result = $node->poll_query_until(
	'postgres',
	"SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'datachecksumsworker%';",
	'0');
is($result, 1, 'await datachecksums worker/launcher termination');

$result = $node->safe_psql('postgres', "SELECT pg_disable_data_checksums();");
$result = $node->poll_query_until(
	'postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'off');
is($result, 1, 'ensure checksums are turned off');

done_testing();
