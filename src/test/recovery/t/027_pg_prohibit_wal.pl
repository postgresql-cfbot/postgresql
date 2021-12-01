
# Copyright (c) 2021, PostgreSQL Global Development Group

# Test wal prohibited state.
use strict;
use warnings;
use FindBin;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Test::More tests => 22;

# Query to read wal_prohibited GUC
my $show_wal_prohibited_query = "SELECT current_setting('wal_prohibited')";

# Initialize database node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(has_archiving => 1, allows_streaming => 1);
$node_primary->start;

# Create few tables and insert some data
$node_primary->safe_psql('postgres',  <<EOSQL);
CREATE TABLE tab AS SELECT 1 AS i;
CREATE UNLOGGED TABLE unlogtab AS SELECT 1 AS i;
EOSQL

# Change to WAL prohibited
$node_primary->safe_psql('postgres', 'SELECT pg_prohibit_wal(true)');
is($node_primary->safe_psql('postgres', $show_wal_prohibited_query), 'on',
	'server is now wal prohibited');

#
# In wal prohibited state, further table insert will fail.
#
# Note that even though inter into unlogged and temporary table doesn't generate
# wal but the transaction does that insert operation will acquire transaction id
# which is not allowed on wal prohibited system. Also, that transaction's abort
# or commit state will be wal logged at the end which is prohibited as well.
#
my ($stdout, $stderr, $timed_out);
$node_primary->psql('postgres', 'INSERT INTO tab VALUES(2)',
          stdout => \$stdout, stderr => \$stderr);
like($stderr, qr/cannot execute INSERT in a read-only transaction/,
	'server is wal prohibited, table insert is failed');
$node_primary->psql('postgres', 'INSERT INTO unlogtab VALUES(2)',
          stdout => \$stdout, stderr => \$stderr);
like($stderr, qr/cannot execute INSERT in a read-only transaction/,
	'server is wal prohibited, unlogged table insert is failed');

# Get current wal write and latest checkpoint lsn
my $write_lsn = $node_primary->lsn('write');
my $checkpoint_lsn = get_latest_checkpoint_location($node_primary);

# Restart the server, shutdown and starup checkpoint will be skipped.
$node_primary->restart;

is($node_primary->safe_psql('postgres', $show_wal_prohibited_query), 'on',
	'server is wal prohibited after restart too');
is($node_primary->lsn('write'), $write_lsn,
	"no wal writes on server, last wal write lsn : $write_lsn");
is(get_latest_checkpoint_location($node_primary), $checkpoint_lsn,
	"no new checkpoint, last checkpoint lsn : $checkpoint_lsn");

# Change server to WAL permitted
$node_primary->safe_psql('postgres', 'SELECT pg_prohibit_wal(false)');
is($node_primary->safe_psql('postgres', $show_wal_prohibited_query),
	'off', 'server is change to wal permitted');

my $new_checkpoint_lsn = get_latest_checkpoint_location($node_primary);
ok($new_checkpoint_lsn ne $checkpoint_lsn,
	"new checkpoint performed, new checkpoint lsn : $new_checkpoint_lsn");

my $new_write_lsn = $node_primary->lsn('write');
ok($new_write_lsn ne $write_lsn,
	"new wal writes on server, new latest wal write lsn : $new_write_lsn");

# Insert data
$node_primary->safe_psql('postgres', 'INSERT INTO tab VALUES(2)');
is($node_primary->safe_psql('postgres', 'SELECT count(i) FROM tab'), '2',
	'table insert passed');

# Only the superuser and the user who granted permission able to call
# pg_prohibit_wal to change wal prohibited state.
$node_primary->safe_psql('postgres', 'CREATE USER non_superuser');
$node_primary->psql('postgres', 'SELECT pg_prohibit_wal(true)',
	stdout => \$stdout, stderr => \$stderr, extra_params => [ '-U', 'non_superuser' ]);
like($stderr, qr/permission denied for function pg_prohibit_wal/,
	'permission denied to non-superuser for alter wal prohibited state');
$node_primary->safe_psql('postgres', 'GRANT EXECUTE ON FUNCTION pg_prohibit_wal TO non_superuser');
$node_primary->psql('postgres', 'SELECT pg_prohibit_wal(true)',
	stdout => \$stdout, stderr => \$stderr, extra_params => [ '-U', 'non_superuser' ]);
is($node_primary->safe_psql('postgres', $show_wal_prohibited_query), 'on',
	'granted permission to non-superuser, able to alter wal prohibited state');

# back to normal state
$node_primary->psql('postgres', 'SELECT pg_prohibit_wal(false)');

my $psql_timeout = IPC::Run::timer(60);
my ($mysession_stdin, $mysession_stdout, $mysession_stderr) = ('', '', '');
my $mysession = IPC::Run::start(
	[
		'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d',
		$node_primary->connstr('postgres')
	],
	'<',
	\$mysession_stdin,
	'>',
	\$mysession_stdout,
	'2>',
	\$mysession_stderr,
	$psql_timeout);

# Write in transaction and get backend pid
$mysession_stdin .= q[
BEGIN;
INSERT INTO tab VALUES(4);
SELECT $$value-4-inserted-into-tab$$;
];
$mysession->pump until $mysession_stdout =~ /value-4-inserted-into-tab[\r\n]$/;
like($mysession_stdout, qr/value-4-inserted-into-tab/,
	'started write transaction in a session');
$mysession_stdout = '';
$mysession_stderr = '';

# Change to WAL prohibited
$node_primary->safe_psql('postgres', 'SELECT pg_prohibit_wal(true)');
is($node_primary->safe_psql('postgres', $show_wal_prohibited_query), 'on',
	'server is changed to wal prohibited by another session');

# Try to commit open write transaction.
$mysession_stdin .= q[
COMMIT;
];
$mysession->pump;
like($mysession_stderr, qr/FATAL:  WAL is now prohibited/,
	'session with open write transaction is terminated');

# Now stop the primary server in WAL prohibited state and take filesystem level
# backup and set up new server from it.
$node_primary->stop;
my $backup_name = 'my_backup';
$node_primary->backup_fs_cold($backup_name);
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name);
$node_standby->start;

# The primary server is stopped in wal prohibited state, the filesystem level
# copy also be in wal prohibited state
is($node_standby->safe_psql('postgres', $show_wal_prohibited_query), 'on',
	'new server created using backup of a stopped primary is also wal prohibited');

# Start Primary
$node_primary->start;

# Set the new server as standby of primary.
# enable_streaming will create standby.signal file which will take out system
# from wal prohibited state.
$node_standby->enable_streaming($node_primary);
$node_standby->restart;

# Check if the new server has been taken out from the wal prohibited state.
is($node_standby->safe_psql('postgres', $show_wal_prohibited_query),
	'off', 'new server as standby is no longer wal prohibited');

# Recovery server cannot be put into wal prohibited state.
$node_standby->psql('postgres', 'SELECT pg_prohibit_wal(true)',
          stdout => \$stdout, stderr => \$stderr);
like($stderr, qr/cannot execute pg_prohibit_wal\(\) during recovery/,
	'standby server state cannot be changed to wal prohibited');

# Primary is still in wal prohibited state, the further insert will fail.
$node_primary->psql('postgres', 'INSERT INTO tab VALUES(3)',
          stdout => \$stdout, stderr => \$stderr);
like($stderr, qr/cannot execute INSERT in a read-only transaction/,
	'primary server is wal prohibited, table insert is failed');

# Change primary to WAL permitted
$node_primary->safe_psql('postgres', 'SELECT pg_prohibit_wal(false)');
is($node_primary->safe_psql('postgres', $show_wal_prohibited_query),
	'off', 'primary server is change to wal permitted');

# Insert data
$node_primary->safe_psql('postgres', 'INSERT INTO tab VALUES(3)');
is($node_primary->safe_psql('postgres', 'SELECT count(i) FROM tab'), '3',
	'insert passed on primary');

# Wait for standbys to catch up
$node_primary->wait_for_catchup($node_standby, 'write');
is($node_standby->safe_psql('postgres', 'SELECT count(i) FROM tab'), '3',
	'new insert replicated on standby as well');


#
# Get latest checkpoint lsn from control file
#
sub get_latest_checkpoint_location
{
	my ($node) = @_;
	my $data_dir = $node->data_dir;
	my ($stdout, $stderr) = run_command([ 'pg_controldata', $data_dir ]);
	my @control_data = split("\n", $stdout);

	my $latest_checkpoint_lsn = undef;
	foreach (@control_data)
	{
		if ($_ =~ /^Latest checkpoint location:\s*(.*)$/mg)
		{
			$latest_checkpoint_lsn = $1;
			last;
		}
	}
	die "No latest checkpoint location in control file found\n"
	unless defined($latest_checkpoint_lsn);

	return $latest_checkpoint_lsn;
}
