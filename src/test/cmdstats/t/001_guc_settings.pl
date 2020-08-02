# Minimal test of cmdstats GUC default settings
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 3;

my ($node, $result);

$node = get_new_node("cmdstats_guc_test");
$node->init;
$node->start;

# Check that by default cmdstats_tracking is off
$result = $node->safe_psql('postgres', "SHOW cmdstats_tracking");
is($result, 'off', 'check default setting of cmdstats_tracking');

# Verify that a mere reload cannot change cmdstats_tracking
$node->append_conf('postgresql.conf', "cmdstats_tracking = true");
$node->reload;
$result = $node->safe_psql('postgres', "SHOW cmdstats_tracking");
is($result, 'off', 'check default setting of cmdstats_tracking');

# Verify that a restart does change cmdstats_tracking
$node->restart;
$result = $node->safe_psql('postgres', "SHOW cmdstats_tracking");
is($result, 'on', 'check default setting of cmdstats_tracking');

$node->stop;
$node->teardown_node;
$node->clean_node;
