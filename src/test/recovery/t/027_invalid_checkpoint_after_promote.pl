use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep nanosleep);
use Test::More tests => 5;

# initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('master');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf(
	'postgresql.conf', q[
checkpoint_timeout = 30s
max_wal_size = 16GB
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
$node_primary->safe_psql('postgres', 'create table test (a int, b varchar(255))');

# use background psql to insert batch, just to make checkpoint a little slow.
my $psql_timeout = IPC::Run::timer(3600);
my ($stdin, $stdout, $stderr) = ('', '', '');
my $psql_primary = IPC::Run::start(
	[ 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d', $node_primary->connstr('postgres') ],
	'<',
	\$stdin,
	'>',
	\$stdout,
	'2>',
	\$stderr,
	$psql_timeout);
$stdin .= q[
INSERT INTO test SELECT i, 'aaaaaaaaaaaaaaaaaaaaaaa' from generate_series(1, 100000000) as i;
];
$psql_primary->pump_nb();

# wait until restartpoint on standy
my $logstart = -s $node_standby->logfile;
my $checkpoint_start = 0;
for (my $i = 0; $i < 3000; $i++)
{
    my $log = slurp_file($node_standby->logfile, $logstart);
	if ($log =~ m/restartpoint starting: time/)
	{
        $checkpoint_start = 1;
		last;
	}
	usleep(100_000);
}
is($checkpoint_start, 1, 'restartpoint has started');

# promote during restartpoint
$node_primary->stop;
$node_standby->promote;

# wait until checkpoint on new primary
$logstart = -s $node_standby->logfile;
$checkpoint_start = 0;
for (my $i = 0; $i < 3000; $i++)
{
    my $log = slurp_file($node_standby->logfile, $logstart);
	if ($log =~ m/restartpoint complete/)
	{
        $checkpoint_start = 1;
		last;
	}
	usleep(100_000);
}
is($checkpoint_start, 1, 'checkpoint has started');

# kill SIGKILL a backend, and all backend will restart. Note that previous checkpoint has not completed.
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

# after recovery, the server will not start, and log PANIC: could not locate a valid checkpoint record
for (my $i = 0; $i < 30; $i++)
{
    ($ret, $stdout, $stderr) = $node_standby->psql('postgres', 'select 1');
    last if $ret == 0;
	sleep(1);
}
is($ret, 0, "psql connect success");
is($stdout, 1, "psql select 1");

$psql_primary->finish;
$killme->finish;
