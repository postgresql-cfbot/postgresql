
# Copyright (c) 2024, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my ($node, $result);

$node = PostgreSQL::Test::Cluster->new('injection_points');
$node->init;
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION injection_points));

$result = $node->psql('postgres', q(select injection_points_attach('FIRST','wait')));
is($result, '0', 'wait injection point set');

my $bg = $node->background_psql('postgres');

$bg->query_until(
	qr/start/, q(
\echo start
select injection_points_run('FIRST');
select injection_points_attach('SECOND','wait');
));

$result = $node->psql('postgres', q(
select injection_points_run('SECOND');
select injection_points_detach('FIRST');
));
is($result, '0', 'wait injection point set');

$bg->quit;

$node->safe_psql('postgres', q(select injection_points_attach('read_test_multixact','wait')));
$node->safe_psql('postgres', q(select injection_points_attach('GetMultiXactIdMembers-CV-sleep','notice')));

my $observer = $node->background_psql('postgres');

$observer->query_safe(
	q(
select read_test_multixact(create_test_multixact());
));

$node->safe_psql('postgres', q(select injection_points_attach('GetNewMultiXactId-done','wait')));

my $creator = $node->background_psql('postgres');

$creator->query_safe( q(select create_test_multixact();));

$node->safe_psql('postgres', q(select injection_points_detach('read_test_multixact')));

$node->safe_psql('postgres', q(select injection_points_detach('GetNewMultiXactId-done')));

$observer->quit;

$creator->quit;

$node->stop;
done_testing();
