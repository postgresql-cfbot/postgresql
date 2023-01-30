
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use Test::More;
use IPC::Run;

my ($node, $port, $stdout, $stderr);

$node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

$node->safe_psql('postgres',
    'CREATE TABLE test(loid oid);'
    . 'INSERT INTO test(loid)
       SELECT lo_creat(27);'
    . 'DROP TABLE test;'
    . 'CREATE TABLE test(loid oid);'
    . 'INSERT INTO test(loid)
       SELECT lo_creat(27);');

$port = $node->port;
IPC::Run::run [ 'vacuumlo', '-v', '-n', '-p', $port, 'postgres' ], '>', \$stdout;

like($stdout, qr/Removing lo  \d+/, 'remove lo successfully');

done_testing();
