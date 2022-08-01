
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;


my $node = PostgreSQL::Test::Cluster->new('main');
my $result;

$node->init;
$node->append_conf('postgresql.conf',
    qq{shared_preload_libraries = 'pg_prewarm'
    pg_prewarm.autoprewarm = true
    pg_prewarm.autoprewarm_interval = 0});
$node->start;

$node->safe_psql(
    "postgres",
    "CREATE EXTENSION pg_prewarm;"
);

# create table
$node->safe_psql(
    "postgres",
    "CREATE TABLE test(c1 int);"
    . "INSERT INTO test
       SELECT generate_series(1, 100);"
);

# test prefetch mode
SKIP:
{
    skip "prefetch is not supported by this build", 1
        if (!check_pg_config("#USE_PREFETCH 1"));

    $result = $node->safe_psql(
        "postgres",
        "SELECT pg_prewarm('test', 'prefetch');");
}

# test read mode
$node->safe_psql(
    "postgres",
    "SELECT pg_prewarm('test', 'read');");

# test buffer_mode
$node->safe_psql(
    "postgres",
    "SELECT pg_prewarm('test', 'buffer');");

$node->restart;

$node->wait_for_log("autoprewarm successfully prewarmed [1-9]+ of [1-9]+ previously-loaded blocks");

$node->stop;

done_testing();

