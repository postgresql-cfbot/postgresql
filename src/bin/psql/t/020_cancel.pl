use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 2;

my $tempdir = TestLib::tempdir;

my $node = get_new_node('main');
$node->init;
$node->start;

# test query canceling by sending SIGINT to a running psql
SKIP: {
	skip "cancel test requires a Unix shell", 2 if $windows_os;

	# use a clean environment
	local %ENV = $node->_get_env();

	# send a kill after 1 second
	local $SIG{ALRM} = sub {
		my $psql_pid = TestLib::slurp_file("$tempdir/psql.pid");
		kill 'INT', $psql_pid;
	};
	alarm 1;

	# run psql to export its pid and wait for the kill
	my $stdin = "\\! echo \$PPID >$tempdir/psql.pid\nselect pg_sleep(5);";
	my ($stdout, $stderr);
	my $result = IPC::Run::run(['psql', '-v', 'ON_ERROR_STOP=1'], '<', \$stdin, '>', \$stdout, '2>', \$stderr);

	# query must have been interrupted
	ok(!$result, 'query failed');
	like($stderr, qr/canceling statement due to user request/, 'query was canceled');
}

$node->stop();
done_testing();
