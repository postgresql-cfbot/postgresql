use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('master');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf(
	'postgresql.conf', q[
checkpoint_timeout = 30s
log_checkpoints = on
restart_after_crash = on
]);
$node_primary->start;
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# setup a standby
my $node_standby = PostgreSQL::Test::Cluster->new('standby1');
$node_standby->init_from_backup($node_primary, $backup_name, has_streaming => 1);
$node_standby->start;

# dummy table for the upcoming tests.
$node_primary->safe_psql('postgres', 'checkpoint');
$node_primary->safe_psql('postgres', 'CREATE TABLE prim_tab (a int);');

# Register a injection point on the standby so as the follow-up
# restart point running on it will wait.
$node_primary->safe_psql('postgres', 'CREATE EXTENSION injection_points;');
# Wait until the extension has been created on the standby
$node_primary->wait_for_replay_catchup($node_standby);
# This causes a restartpoint to wait on a standby.
$node_standby->safe_psql('postgres',
  "SELECT injection_points_attach('CreateRestartPoint', 'wait');");

# Execute a restart point on the standby, that will be waited on.
# This needs to be in the background as we'll wait on it.
my $logstart = -s $node_standby->logfile;
my $psql_session =
  $node_standby->background_psql('postgres', on_error_stop => 0);
$psql_session->query_until(qr/starting_checkpoint/, q(
   \echo starting_checkpoint
   CHECKPOINT;
));

# Switch one WAL segment to make the restartpoint remove it.
$node_primary->safe_psql('postgres', 'INSERT INTO prim_tab VALUES (1);');
$node_primary->safe_psql('postgres', 'SELECT pg_switch_wal();');
$node_primary->wait_for_replay_catchup($node_standby);

# Wait until the checkpointer is in the middle of the restartpoint
# processing.
ok( $node_standby->poll_query_until(
	'postgres',
	qq[SELECT count(*) FROM pg_stat_activity
           WHERE backend_type = 'checkpointer' AND wait_event = 'injection_wait' ;],
	'1'),
    'checkpointer is waiting at restart point'
    ) or die "Timed out while waiting for checkpointer to run restartpoint";


# Restartpoint should have started on standby.
my $log = slurp_file($node_standby->logfile, $logstart);
my $checkpoint_start = 0;
if ($log =~ m/restartpoint starting: immediate wait/)
{
	$checkpoint_start = 1;
}
is($checkpoint_start, 1, 'restartpoint has started');

# promote during restartpoint
$node_primary->stop;
$node_standby->promote;

# Update the start position before waking up the checkpointer!
$logstart = -s $node_standby->logfile;

# Now wake up the checkpointer
$node_standby->safe_psql('postgres',
  "SELECT injection_points_wake();");

# wait until checkpoint completes on the newly-promoted standby.
my $checkpoint_complete = 0;
for (my $i = 0; $i < 3000; $i++)
{
	my $log = slurp_file($node_standby->logfile, $logstart);
	if ($log =~ m/restartpoint complete/)
	{
		$checkpoint_complete = 1;
		last;
	}
	usleep(100_000);
}
is($checkpoint_complete, 1, 'restartpoint has completed');

# kill SIGKILL a backend, and all backend will restart. Note that previous
# checkpoint has not completed.
my $psql_timeout = IPC::Run::timer(3600);
my ($killme_stdin, $killme_stdout, $killme_stderr) = ('', '', '');
my $killme = IPC::Run::start(
	[ 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d', $node_standby->connstr('postgres') ],
	'<',
	\$killme_stdin,
	'>',
	\$killme_stdout,
	'2>',
	\$killme_stderr,
	$psql_timeout);
$killme_stdin .= q[
SELECT pg_backend_pid();
];
$killme->pump until $killme_stdout =~ /[[:digit:]]+[\r\n]$/;
my $pid = $killme_stdout;
chomp($pid);
my $ret = PostgreSQL::Test::Utils::system_log('pg_ctl', 'kill', 'KILL', $pid);
is($ret, 0, 'killed process with KILL');
my $stdout;
my $stderr;

# After recovery, the server should be able to start.
for (my $i = 0; $i < 30; $i++)
{
    ($ret, $stdout, $stderr) = $node_standby->psql('postgres', 'select 1');
    last if $ret == 0;
	sleep(1);
}
is($ret, 0, "psql connect success");
is($stdout, 1, "psql select 1");

done_testing();
