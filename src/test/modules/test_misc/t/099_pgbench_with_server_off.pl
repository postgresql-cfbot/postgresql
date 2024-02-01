
# Copyright (c) 2021-2023, PostgreSQL Global Development Group

#
# test pgbench with server stopped abruptly during the pgbench run
#

use strict;
use warnings;

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');

$node->init();
$node->start;

my $pb_timeout =
  IPC::Run::timer($PostgreSQL::Test::Utils::timeout_default);
my ($pb_stdin, $pb_stdout, $pb_stderr) = ('', '', '');

$ENV{PGDATABASE} = 'postgres';
$ENV{PGHOST} = $node->host;
$ENV{PGPORT} = $node->port;

my $pb = IPC::Run::start(
    [
        'pgbench',
        '-i', '-s', '1000'
    ],
    '<',
    \$pb_stdin,
    '>',
    \$pb_stdout,
    '2>',
    \$pb_stderr,
    $pb_timeout);
sleep(5);
$node->stop('immediate');
ok( pump_until(
        $pb, $pb_timeout,
        \$pb_stderr, qr/PQputline failed/),
    'pgbench reacted to server shutdown during copy');
$pb->finish();

done_testing();
