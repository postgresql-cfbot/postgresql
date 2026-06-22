# Copyright (c) 2025-2026, PostgreSQL Global Development Group
#
# Regression test for the ProcKill lock-group vs. procLatch recycle race.
#
# Two backends form a lock group, then are terminated concurrently.  The test
# uses injection points placed inside ProcKill() to pause both victims there
# and verify that a freshly forked backend can claim the recycled PGPROC slot
# without hitting "latch already owned by PID ..." PANIC.
#
# Requires --enable-injection-points.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use Time::HiRes qw(usleep);

use constant PANIC_RE => qr/PANIC:\s+latch already owned by PID/;

if (($ENV{enable_injection_points} // '') ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('prockill_race');
$node->init;
$node->append_conf('postgresql.conf',
	q{shared_preload_libraries = 'injection_points'});
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

# Form a two-backend lock group.
my $leader   = $node->background_psql('postgres');
my $follower = $node->background_psql('postgres');

$leader->query_safe('SELECT prockill_become_lock_group_leader()');
my $leader_pid   = $leader->query_safe('SELECT pg_backend_pid()');
my $follower_pid = $follower->query_safe('SELECT pg_backend_pid()');
$leader_pid   =~ s/\s+//g;
$follower_pid =~ s/\s+//g;

$follower->query_safe("SELECT prockill_become_lock_group_member($leader_pid)");

# Attach injection waits from a throwaway controller session, not from
# $leader/$follower.  injection_points_attach() registers a before_shmem_exit
# cleanup that would fire when the controller's session ends (and, if called
# from a victim, before ProcKill ever runs).  prockill_attach_injection_wait()
# calls InjectionPointAttach() directly, leaving no such hook.
#
# Two distinct points are placed inside the lock-group block in ProcKill:
# the leader fires 'prockill-after-lockgroup-leader' and the follower fires
# 'prockill-after-lockgroup-member', so the test can park each victim
# independently.
$node->safe_psql('postgres',
	"SELECT prockill_attach_injection_wait_pid('prockill-after-lockgroup-leader', $leader_pid)");
$node->safe_psql('postgres',
	"SELECT prockill_attach_injection_wait_pid('prockill-after-lockgroup-member', $follower_pid)");

# Third injection point, scoped to the leader's PID.  This sits between the
# lock-group block and the post-block DisownLatch in proc.c.  After both
# victims wake from their respective lock-group points, the leader will stall
# here while its procLatch is still owned, and the follower (whose PID does
# not match the condition) will run through to the consolidated freelist
# push, publishing the leader's PGPROC with the latch still in the owned
# state.  This is what turns the otherwise empirical race into a
# deterministic one on branches where DisownLatch is not yet hoisted to the
# top of ProcKill.
$node->safe_psql('postgres',
	"SELECT prockill_attach_injection_wait_pid('prockill-pre-disown-latch', $leader_pid)");

# Cap the default poll_query_until timeout: if the lock-group fix has
# regressed the postmaster will PANIC mid-cleanup, and we don't want to
# spend 180s polling against a dead server.
local $PostgreSQL::Test::Utils::timeout_default = 5;

# Kill the leader and wait for it to pause inside ProcKill.
# prockill_backend_in_injection() reads PGPROC->wait_event_info directly
# because pg_stat_activity and ProcArray are already torn down before ProcKill.
$node->safe_psql('postgres', "SELECT pg_terminate_backend($leader_pid)");
my $leader_ok = $node->poll_query_until(
	'postgres',
	"SELECT prockill_backend_in_injection($leader_pid, 'prockill-after-lockgroup-leader')"
);

# Kill the follower.  Once the follower starts cleanup, one of two things
# happens: the follower parks at the injection point (fix in place), or a
# probe backend grabs the recycled PGPROC slot and PANICs (fix regressed).
# On the PANIC path the postmaster crashes, so subsequent SQL probes return
# nothing but connection errors -- the live query side has no way to signal
# "the race fired".  Scanning the server log for PANIC_RE is the only
# positive evidence of the regression; the dual-condition loop below exits
# as soon as either condition is observable.
eval {
	$node->safe_psql('postgres',
		"SELECT pg_terminate_backend($follower_pid)");
};
my $follower_ok = 0;
my $deadline = time() + 2;
while (time() < $deadline)
{
	last if slurp_file($node->logfile) =~ PANIC_RE;
	$follower_ok = eval {
		$node->safe_psql('postgres',
			"SELECT prockill_backend_in_injection($follower_pid, 'prockill-after-lockgroup-member')")
		  eq 't';
	} // 0;
	last if $follower_ok;
	usleep(50_000);
}

# Release both victims (no-op if the postmaster has already crashed).
eval {
	$node->safe_psql('postgres',
		"SELECT injection_points_wakeup('prockill-after-lockgroup-leader')");
};
eval {
	$node->safe_psql('postgres',
		"SELECT injection_points_wakeup('prockill-after-lockgroup-member')");
};

# Release the leader from the third injection point too (the follower never
# parked there because of the PID-scoped condition).  No-op on crash.
eval {
	$node->safe_psql('postgres',
		"SELECT injection_points_wakeup('prockill-pre-disown-latch')");
};

# A new backend must be able to claim the recycled PGPROC slot without PANIC.
my $select_ok = eval { $node->safe_psql('postgres', 'SELECT 1'); 1 };

# Decisive check: scan the server log for the exact PANIC the fix prevents.
# This turns a flaky/slow failure mode into a deterministic one-line diagnosis.
my $log_contents = slurp_file($node->logfile);
ok($log_contents !~ PANIC_RE,
	'no "latch already owned" PANIC in server log (regression of ProcKill lock-group fix)');

ok($leader_ok && $follower_ok,
	'leader and follower reached ProcKill injection points');
ok($select_ok,
	'new backend claimed recycled PGPROC without latch-recycle PANIC');

eval {
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('prockill-after-lockgroup-leader')");
};
eval {
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('prockill-after-lockgroup-member')");
};
eval {
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('prockill-pre-disown-latch')");
};

eval { $leader->quit };
eval { $follower->quit };
$node->stop('fast', fail_ok => 1);

done_testing();
