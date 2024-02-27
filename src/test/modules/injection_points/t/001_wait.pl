
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

# Test for Multixact generation edge case
$node->safe_psql('postgres', q(select injection_points_attach('read_test_multixact','wait')));
$node->safe_psql('postgres', q(select injection_points_attach('GetMultiXactIdMembers-CV-sleep','notice')));

# This session must observe sleep on CV when generating multixact.
# To achive this it first will create a multixact, then pause before reading it.
my $observer = $node->background_psql('postgres');

$observer->query_until(qr/start/,
q(
	\echo start
	select read_test_multixact(create_test_multixact());
));

# This session will create next Multixact, it's necessary to avoid edge case 1 (see multixact.c)
my $creator = $node->background_psql('postgres');
$node->safe_psql('postgres', q(select injection_points_attach('GetNewMultiXactId-done','wait')));

# We expect this query to hand in critical section after generating new multixact,
# but before filling it's offset into SLRU
$creator->query_until(qr/start/, q(
	\echo start
	select create_test_multixact();
));

# Now we are sure we can reach edge case 2. Proceed session that is reading that multixact.
$node->safe_psql('postgres', q(select injection_points_detach('read_test_multixact')));

# Release critical section. We have to do this so everyon can proceed.
# But this is inherent race condition, I hope the tast will not be unstable here.
# The only way to stabilize it will be adding some sleep here.
$node->safe_psql('postgres', q(select injection_points_detach('GetNewMultiXactId-done')));

# Here goes the whole purpose of this test: see that sleep in fact occured.
ok( pump_until(
		$observer->{run}, $observer->{timeout},
		\$observer->{stderr}, qr/notice triggered for injection point GetMultiXactIdMembers-CV-sleep/),
	"sleep observed");

$observer->quit;

$creator->quit;

$node->stop;
done_testing();
