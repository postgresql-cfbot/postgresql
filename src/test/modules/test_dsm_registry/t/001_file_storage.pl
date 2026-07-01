# Copyright (c) 2023-2025, PostgreSQL Global Development Group
use strict;
use warnings FATAL => 'all';
use Config;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');

$node->init();
$node->append_conf('postgresql.conf',
							"shared_preload_libraries = 'test_dsm_registry'");
$node->start();

$node->safe_psql('postgres', "CREATE EXTENSION test_dsm_registry");

my $result;

$node->safe_psql('postgres', "SELECT set_val_in_hash('test-1', '1414')");
$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql('postgres', "SELECT set_val_in_hash('test-2', '1415')");
$node->stop('immediate');
$node->start();

$result = $node->safe_psql('postgres', "SELECT get_val_in_hash('test-1')");
is($result, '1414', "Value inserted before the checkpoint was restored");
$result = $node->safe_psql('postgres', "SELECT get_val_in_hash('test-2')");
is($result, '', "Value inserted after the checkpoint was lost");

done_testing();
