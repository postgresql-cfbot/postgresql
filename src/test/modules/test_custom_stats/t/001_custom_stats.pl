# Copyright (c) 2025-2026, PostgreSQL Global Development Group

# Test custom pgstats functionality
#
# This script includes tests for both variable and fixed-sized custom
# pgstats:
# - Creation, updates, and reporting.
# - Persistence across restarts.
# - Loss after crash recovery.
# - Resets for fixed-sized stats.
#
# Variable-sized stats are tested with both own_hash=true (dedicated
# dshash) and own_hash=false (shared dshash) to cover both paths.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Copy;

my $result;
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'test_custom_var_stats, test_custom_fixed_stats'"
);
$node->start;

$node->safe_psql('postgres', q(CREATE EXTENSION test_custom_var_stats));
$node->safe_psql('postgres', q(CREATE EXTENSION test_custom_fixed_stats));

# Test variable-sized stats with both own_hash configurations (true and false).
foreach my $use_own_hash (qw(true false))
{

	# Restart with the appropriate setting.
	$node->append_conf('postgresql.conf',
		"test_custom_var_stats.use_own_hash = $use_own_hash");
	$node->restart;

	$result = $node->safe_psql('postgres',
		q(select test_custom_stats_var_is_own_hash()));
	is( $result,
		$use_own_hash eq 'true' ? 't' : 'f',
		"check if dedicated hash is allocated (own_hash=$use_own_hash)");

	# Create entries for variable-sized stats.
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_create('entry1', 'Test entry 1')));
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_create('entry2', 'Test entry 2')));
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_create('entry3', 'Test entry 3')));
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_create('entry4', 'Test entry 4')));

	# Update counters: entry1=2, entry2=3, entry3=2, entry4=3
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_update('entry1')));
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_update('entry1')));
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_update('entry2')));
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_update('entry2')));
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_update('entry2')));
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_update('entry3')));
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_update('entry3')));
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_update('entry4')));
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_update('entry4')));
	$node->safe_psql('postgres',
		q(select test_custom_stats_var_update('entry4')));

	# Test data reports.
	$result = $node->safe_psql('postgres',
		q(select * from test_custom_stats_var_report('entry1')));
	is( $result,
		"entry1|2|Test entry 1",
		"report for variable-sized data of entry1");

	$result = $node->safe_psql('postgres',
		q(select * from test_custom_stats_var_report('entry2')));
	is( $result,
		"entry2|3|Test entry 2",
		"report for variable-sized data of entry2");

	$result = $node->safe_psql('postgres',
		q(select * from test_custom_stats_var_report('entry3')));
	is( $result,
		"entry3|2|Test entry 3",
		"report for variable-sized data of entry3");

	$result = $node->safe_psql('postgres',
		q(select * from test_custom_stats_var_report('entry4')));
	is( $result,
		"entry4|3|Test entry 4",
		"report for variable-sized data of entry4");

	# Test drop of variable-sized stats.
	$node->safe_psql('postgres',
		q(select * from test_custom_stats_var_drop('entry3')));
	$result = $node->safe_psql('postgres',
		q(select * from test_custom_stats_var_report('entry3')));
	is($result, "", "entry3 not found after drop");
	$node->safe_psql('postgres',
		q(select * from test_custom_stats_var_drop('entry4')));
	$result = $node->safe_psql('postgres',
		q(select * from test_custom_stats_var_report('entry4')));
	is($result, "", "entry4 not found after drop");
}

# fixed-sized stats are updated 3 times, so the report below should match.
$node->safe_psql('postgres', q(select test_custom_stats_fixed_update()));
$node->safe_psql('postgres', q(select test_custom_stats_fixed_update()));
$node->safe_psql('postgres', q(select test_custom_stats_fixed_update()));

$result = $node->safe_psql('postgres',
	q(select * from test_custom_stats_fixed_report()));
is($result, "3|", "report for fixed-sized stats");

# Test persistence across clean restart.
$node->stop();
$node->start();

$result = $node->safe_psql('postgres',
	q(select * from test_custom_stats_var_report('entry1')));
is( $result,
	"entry1|2|Test entry 1",
	"variable-sized stats persist after clean restart");

$result = $node->safe_psql('postgres',
	q(select * from test_custom_stats_var_report('entry2')));
is( $result,
	"entry2|3|Test entry 2",
	"variable-sized stats persist after clean restart");

$result = $node->safe_psql('postgres',
	q(select * from test_custom_stats_fixed_report()));
is($result, "3|", "fixed-sized stats persist after clean restart");

# Test persistence after crash recovery.
$node->stop('immediate');
$node->start;

$result = $node->safe_psql('postgres',
	q(select * from test_custom_stats_var_report('entry1')));
is($result, "", "variable-sized stats of entry1 lost after crash recovery");
$result = $node->safe_psql('postgres',
	q(select * from test_custom_stats_var_report('entry2')));
is($result, "", "variable-sized stats of entry2 lost after crash recovery");

# Crash recovery sets the reset timestamp.
$result = $node->safe_psql('postgres',
	q(select numcalls from test_custom_stats_fixed_report() where stats_reset is not null)
);
is($result, "0", "fixed-sized stats are reset after crash recovery");

# Test reset of fixed-sized stats.
$node->safe_psql('postgres', q(select test_custom_stats_fixed_update()));
$node->safe_psql('postgres', q(select test_custom_stats_fixed_update()));
$node->safe_psql('postgres', q(select test_custom_stats_fixed_update()));

$result = $node->safe_psql('postgres',
	q(select numcalls from test_custom_stats_fixed_report()));
is($result, "3", "report of fixed-sized before manual reset");

$node->safe_psql('postgres', q(select test_custom_stats_fixed_reset()));

$result = $node->safe_psql('postgres',
	q(select numcalls from test_custom_stats_fixed_report() where stats_reset is not null)
);
is($result, "0", "report of fixed-sized after manual reset");

# Test completed successfully
done_testing();
