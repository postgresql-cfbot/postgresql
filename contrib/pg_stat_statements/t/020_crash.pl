# Copyright (c) 2023-2024, PostgreSQL Global Development Group

# Tests for checking that pg_stat_statements contents are preserved
# across restarts.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	q[
		shared_preload_libraries = 'pg_stat_statements'
		restart_after_crash = 1
	]);
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_statements');

# Without the CHECKPOINT hook, we won't see this query in pg_stat_statements
# after a server crash.
$node->safe_psql('postgres', 'CREATE TABLE t1 (a int)');

$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql('postgres', 'SELECT a FROM t1');

is( $node->safe_psql(
		'postgres',
		"SELECT query FROM pg_stat_statements WHERE query NOT LIKE '%pg_stat_statements%' ORDER BY query"
	),
	"CHECKPOINT\nCREATE TABLE t1 (a int)\nSELECT a FROM t1",
	'pg_stat_statements populated');


# Perform a server shutdown by killing the backend.
my $psql_timeout = IPC::Run::timer($PostgreSQL::Test::Utils::timeout_default);

my ($killme_stdin, $killme_stdout, $killme_stderr) = ('', '', '');
my $killme = IPC::Run::start(
	[
		'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d',
		$node->connstr('postgres')
	],
	'<',
	\$killme_stdin,
	'>',
	\$killme_stdout,
	'2>',
	\$killme_stderr,
	$psql_timeout);

$killme_stdin .= "SELECT pg_backend_pid();\n";
ok( pump_until(
		$killme, $psql_timeout, \$killme_stdout, qr/[[:digit:]]+[\r\n]$/m),
	'acquired pid for SIGQUIT');
my $pid = $killme_stdout;
chomp($pid);

my $ret = PostgreSQL::Test::Utils::system_log('pg_ctl', 'kill', 'QUIT', $pid);
is($ret, 0, "killed process with SIGQUIT");

$killme->finish;

# Wait till server restarts
is($node->poll_query_until('postgres', undef, ''),
	"1", "reconnected after SIGQUIT");

is( $node->safe_psql(
		'postgres',
		"SELECT query FROM pg_stat_statements WHERE query NOT LIKE '%pg_stat_statements%' ORDER BY query"
	),
	"CHECKPOINT\nCREATE TABLE t1 (a int)\nSELECT a FROM t1\nSELECT pg_backend_pid()",
	'pg_stat_statements data kept across the server crash');

$node->stop;

done_testing();
