# Copyright (c) 2024, PostgreSQL Global Development Group

# This test verifies edge case of reading a multixact:
# when we have multixact that is followed by exactly one another multixact,
# and another multixact have no offest yet, we must wait until this offset
# becomes observable. Previously we used to wait for 1ms in a loop in this
# case, but now we use CV for this. This test is exercising such a sleep.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my ($node, $result);

$node = PostgreSQL::Test::Cluster->new('multixact_CV_sleep');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'test_slru'");
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION injection_points));
$node->safe_psql('postgres', q(CREATE EXTENSION test_slru));

# Test for Multixact generation edge case
$node->safe_psql('postgres', q(select injection_points_attach('test_read_multixact','wait')));
$node->safe_psql('postgres', q(select injection_points_attach('GetMultiXactIdMembers-CV-sleep','wait')));

# This session must observe sleep on CV when generating multixact.
# To achive this it first will create a multixact, then pause before reading it.
my $observer = $node->background_psql('postgres');

# This query will create multixact, and hand just before reading it.
$observer->query_until(qr/start/,
q(
	\echo start
	select test_read_multixact(test_create_multixact());
));
$node->wait_for_event('client backend', 'test_read_multixact');

# This session will create next Multixact, it's necessary to avoid edge case 1
# (see multixact.c)
my $creator = $node->background_psql('postgres');
$node->safe_psql('postgres', q(select injection_points_attach('GetNewMultiXactId-done','wait');));

# We expect this query to hand in critical section after generating new multixact,
# but before filling it's offset into SLRU
$creator->query_until(qr/start/, q(
	\echo start
	select injection_points_load('GetNewMultiXactId-done');
	select test_create_multixact();
));

$node->wait_for_event('client backend', 'GetNewMultiXactId-done');

# Now we are sure we can reach edge case 2.
# Observer is going to read multixact, which has next, but next lacks offset.
$node->safe_psql('postgres', q(select injection_points_wakeup('test_read_multixact')));


$node->wait_for_event('client backend', 'GetMultiXactIdMembers-CV-sleep');

# Now we have two backends waiting in GetNewMultiXactId-done and
# GetMultiXactIdMembers-CV-sleep. Also we have 3 injections points set to wait.
# If we wakeup GetMultiXactIdMembers-CV-sleep it will happend again, so we must
# detach it first. So let's detach all injection points, then wake up all
# backends.

$node->safe_psql('postgres', q(select injection_points_detach('test_read_multixact')));
$node->safe_psql('postgres', q(select injection_points_detach('GetNewMultiXactId-done')));
$node->safe_psql('postgres', q(select injection_points_detach('GetMultiXactIdMembers-CV-sleep')));

$node->safe_psql('postgres', q(select injection_points_wakeup('GetNewMultiXactId-done')));
$node->safe_psql('postgres', q(select injection_points_wakeup('GetMultiXactIdMembers-CV-sleep')));

# Background psql will now be able to read the result and disconnect.
$observer->quit;
$creator->quit;

$node->stop;

# If we reached this point - everything is OK.
ok(1);
done_testing();
