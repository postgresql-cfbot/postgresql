# Check that a canceled or terminated waiter does not leave a stale slot
# behind in the waiter array. A leaked slot would make later wakeups of
# the same injection point bump the leaked slot's counter instead of the
# real waiter's, leaving the real waiter stuck.

setup
{
	CREATE EXTENSION injection_points;
}
teardown
{
	DROP EXTENSION injection_points;
}

# The first waiter, which gets canceled or terminated. No set_local:
# the injection point must survive s1's termination so that s3 can
# still detach it.
session s1
setup	{
	SELECT injection_points_attach('injection-points-wait', 'wait');
}
step wait1	{ SELECT injection_points_run('injection-points-wait'); }

# The second waiter, which must still receive the wakeup.
session s2
step wait2	{ SELECT injection_points_run('injection-points-wait'); }
step noop2	{ }

# The control session. The blocker annotations on cancel3/terminate3
# plus noop3 make the tester wait until wait1 actually finished before
# starting wait2, otherwise wait2 could register its own waiter slot
# while s1 still holds the old one.
session s3
step cancel3	{
	SELECT pg_cancel_backend(pid) FROM pg_stat_activity
	  WHERE wait_event = 'injection-points-wait';
}
step terminate3	{
	SELECT pg_terminate_backend(pid) FROM pg_stat_activity
	  WHERE wait_event = 'injection-points-wait';
}
step wakeup3	{ SELECT injection_points_wakeup('injection-points-wait'); }
step detach3	{ SELECT injection_points_detach('injection-points-wait'); }
step noop3	{ }

permutation wait1 cancel3(wait1) noop3 wait2 wakeup3 noop2 detach3

# The terminate permutation has to stay last: s1's connection is dead
# afterwards, and the tester never reconnects a session.
permutation wait1 terminate3(wait1) noop3 wait2 wakeup3 noop2 detach3
