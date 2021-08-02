use strict;
use warnings;
use Cwd;
use Config;
use File::Path qw(rmtree);
use PostgresNode;
use TestLib;
use Test::More tests => 20;

my $psql_timeout = IPC::Run::timer(60);
my $primary = get_new_node('primary');

# initialize the clusture;
$primary->init(
        allows_streaming => 1,
        auth_extra       => [ '--create-role', 'repl_role' ]);
$primary->append_conf('postgresql.conf', "max_replication_slots = 2");
$primary->start;

# Below testcase verify the syntax and corresponding wal_prohibited values.
$primary->command_ok([ 'psql', '-c', "SELECT pg_prohibit_wal(TRUE);" ],
                    "pg_prohibit_wal(TRUE) syntax verified.");
my $data_check = $primary->safe_psql('postgres', 'SHOW wal_prohibited;');
is($data_check, 'on', "wal_prohibited is 'on' with pg_prohibit_wal(TRUE) verified");

$primary->command_ok([ 'psql', '-c', "SELECT pg_prohibit_wal(FALSE);" ],
                    "pg_prohibit_wal(FALSE) syntax verified.");
my $data_check = $primary->safe_psql('postgres', 'SHOW wal_prohibited;');
is($data_check, 'off', "wal_prohibited is 'off' with pg_prohibit_wal(FALSE) verified");

# Below testcase verify an user can execute CREATE/INSERT statement
# with pg_prohibit_wal(false).
$primary->safe_psql('postgres',  <<EOM);
SELECT pg_prohibit_wal(false);
CREATE TABLE test_tbl (id int);
INSERT INTO test_tbl values(10);
EOM
my $data_check = $primary->safe_psql('postgres', 'SELECT count(id) FROM test_tbl;');
is($data_check, '1', 'CREATE/INSERT statement is working fine with pg_prohibit_wal(false)');

# Below testcase verify an user can not execute CREATE TABLE command
# in a read-only transaction.
$primary->safe_psql('postgres', 'SELECT pg_prohibit_wal(true);');
my ($stdout, $stderr, $timed_out);
$primary->psql('postgres', 'create table test_tbl2 (id int);',
          stdout => \$stdout, stderr => \$stderr);
is($stderr, "psql:<stdin>:1: ERROR:  cannot execute CREATE TABLE in a read-only transaction",
                                  "cannot execute CREATE TABLE in a read-only transaction");

# Below testcase verify if permission denied for function pg_prohibit_wal(true/false).
$primary->safe_psql('postgres', 'SELECT pg_prohibit_wal(false);');
$primary->safe_psql('postgres', 'CREATE USER non_superuser;');
$primary->psql('postgres', 'SELECT pg_prohibit_wal(true);',
          stdout => \$stdout, stderr => \$stderr,
          extra_params => [ '-U', 'non_superuser' ]);
is($stderr, "psql:<stdin>:1: ERROR:  permission denied for function pg_prohibit_wal",
                                    "permission denied for function pg_prohibit_wal");

# Below testcase verify the GRANT/REVOKE permission on function
# pg_prohibit_wal TO/FROM non_superuser.
# if non_superuser can execute pg_prohibit_wal function only after getting execute permission
$primary->command_ok([ 'psql', '-c', "GRANT EXECUTE ON FUNCTION pg_prohibit_wal TO non_superuser;" ],
                    "Grant permission on function pg_prohibit_wal to non_superuser verified");

$primary->command_ok([ 'psql', '-U', 'non_superuser', '-d', 'postgres', '-c', "SELECT pg_prohibit_wal(true);" ],
                    "Non_superuser can execute pg_prohibit_wal(true) after getting execute permission");

# Below testcase verify 'WAL write prohibited' from the 'pg_controldata'
# when "pg_prohibit_wal(true)" and START/STOP/RESTART the server.
$primary->safe_psql('postgres', 'SELECT pg_prohibit_wal(true);');
my $primary_data = $primary->data_dir;
my ($res, $controldataparm) = ('','');

$res = get_controldata_parm('WAL write prohibited');
is( $res, 'WAL write prohibited:                 yes', "Verified, 'WAL write prohibited: yes' after pg_prohibit_wal(TRUE).");

$primary->stop;
$primary->start;
$res = get_controldata_parm('WAL write prohibited');
is( $res, 'WAL write prohibited:                 yes', "Verified, 'WAL write prohibited: yes' after pg_prohibit_wal(TRUE) and STOP/START Server.");

$primary->restart;
$res = get_controldata_parm('WAL write prohibited');
is( $res, 'WAL write prohibited:                 yes', "Verified, 'WAL write prohibited: yes' after pg_prohibit_wal(TRUE) and RESTART Server.");

# Below testcase verify 'WAL write prohibited' from the 'pg_controldata with adding STANDBY.SIGNAL'
# when "pg_prohibit_wal(true)" and RESTART the server.
system("touch $primary_data/standby.signal");
$primary->restart;
$res = get_controldata_parm('WAL write prohibited');
is( $res, 'WAL write prohibited:                 no', "Verified, 'WAL write prohibited: no' after pg_prohibit_wal(TRUE) with adding STANDBY.SIGNAL and RESTART Server.");

system("rm -rf $primary_data/standby.signal");
$primary->restart;

$primary->safe_psql('postgres', 'SELECT pg_prohibit_wal(false);');

# Below test will verify, open a transaction in one session, which perform write
# operation (e.g create/insert)  and from another session execute pg_prohibit_wal(true)
# this should kill transaction.

# Sessio1: Execute some transaction.
my ($session1_stdin, $session1_stdout, $session1_stderr) = ('', '', '');
my $session1 = IPC::Run::start(
        [
                'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d',
                $primary->connstr('postgres')
        ],
        '<',
        \$session1_stdin,
        '>',
        \$session1_stdout,
        '2>',
        \$session1_stderr,
    $psql_timeout);

$session1_stdin .= q[
BEGIN;
INSERT INTO test_tbl (SELECT generate_series(1,1000,2));
];

$session1->run();
# Session2: Execute pg_prohibit_wal(TRUE)
$primary->safe_psql('postgres', 'SELECT pg_prohibit_wal(TRUE);');


ok( pump_until(
                $session1,
                \$session1_stderr,
                qr/psql:<stdin>:3: ERROR:  cannot execute INSERT in a read-only transaction/m
        ),
        "Verified, cannot execute INSERT in a read-only transaction");

$session1->finish;

sub pump_until
{
        my ($proc, $stream, $untl) = @_;
        $proc->pump_nb();
        while (1)
        {
                last if $$stream =~ /$untl/;
                if ($psql_timeout->is_expired)
                {
                        diag("aborting wait: program timed out");
                        diag("stream contents: >>", $$stream, "<<");
                        diag("pattern searched for: ", $untl);

                        return 0;
                }
                if (not $proc->pumpable())
                {
                        diag("aborting wait: program died");
                        diag("stream contents: >>", $$stream, "<<");
                        diag("pattern searched for: ", $untl);

                        return 0;
                }
                $proc->pump();
        }
        return 1;
}

sub get_controldata_parm
{
  my $param_name = @_;
  my ($stdout, $stderr) = run_command([ 'pg_controldata', $primary_data ]);
  my @control_data = split("\n", $stdout);
  foreach (@control_data)
  {
    if (index($_, $_[0]) != -1)
    {
        diag("pg_controldata content: >>", $_, "\n");
        $controldataparm = $_;
    }
  }
  return $controldataparm;
}

$primary->restart;

# Below testcase verify an user can execute CREATE TABLE AS SELECT statement
# with pg_prohibit_wal(false).

$primary->safe_psql('postgres', "SELECT pg_prohibit_wal(FALSE);" );
$primary->safe_psql('postgres', "CREATE TABLE test_tbl2 AS SELECT generate_series(1,12) AS a");

my $data_check = $primary->safe_psql('postgres', 'SELECT count(a) FROM test_tbl2;');
is($data_check, '12', 'CREATE TABLE statement is working fine with pg_prohibit_wal(false) on master');

my $backup_name = 'my_backup';

# Take backup
$primary->backup($backup_name);

# Create streaming standby
# standby_1 -> primary
# standby_2 -> standby_1
my $node_standby_1 = get_new_node('standby_1');
$node_standby_1->init_from_backup($primary, $backup_name,
        has_streaming => 1);
$node_standby_1->start;
$node_standby_1->backup($backup_name);

my $node_standby_2 = get_new_node('standby_2');
$node_standby_2->init_from_backup($node_standby_1, $backup_name,
        has_streaming => 1);
$node_standby_2->start;
$node_standby_2->backup($backup_name);

# Below testcase verify MASTER/SLAVE  replication working fine and validate data
my $data_check = $primary->safe_psql('postgres', 'select pg_is_in_recovery();');
is($data_check, 'f', 'Master: Streaming Replication is working fine.');

my $data_check = $node_standby_1->safe_psql('postgres', 'select pg_is_in_recovery();');
is($data_check, 't', 'Slave: Streaming Replication is working fine.');

my $data_check = $node_standby_2->safe_psql('postgres', 'select pg_is_in_recovery();');
is($data_check, 't', 'Slave: Streaming Replication is working fine.');

my $data_check = $node_standby_1->safe_psql('postgres', 'SELECT count(a) FROM test_tbl2;');
is($data_check, '12', 'Check the streamed content on node_standby_1');

# Below testcase verify an user in standby "cannot execute pg_prohibit_wal() during recovery"
my ($stdout, $stderr, $timed_out);
$node_standby_1->psql('postgres', 'SELECT pg_prohibit_wal(TRUE);',
          stdout => \$stdout, stderr => \$stderr);
is($stderr, "psql:<stdin>:1: ERROR:  cannot execute pg_prohibit_wal() during recovery",
                                    "cannot execute pg_prohibit_wal() during recovery");

# stop server
$node_standby_1->stop;
$node_standby_2->stop;
$primary->stop;
