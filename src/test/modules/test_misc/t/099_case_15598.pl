# Run ecpg regression tests in a loop
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Basename;

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('p1');
$node->init(
);

$node->append_conf(
	'postgresql.conf',
	qq{
});

$node->start;

$node->safe_psql('postgres', 'select 1');

my $psql_timeout = IPC::Run::timer(10);
# Run psql, ...
my ($psql_stdin, $psql_stdout, $psql_stderr) = ('', '', '');
my $psql = IPC::Run::start(
    [
        'psql', '-X', '-At', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d',
        $node->connstr('postgres')
    ],
    '<',
    \$psql_stdin,
    '>',
    \$psql_stdout,
    '2>',
    \$psql_stderr,
    $psql_timeout);

$psql_stdin .= q[
SET idle_in_transaction_session_timeout=500;
BEGIN;
SELECT * FROM pg_class;
];

$psql->pump_nb();
sleep(1);

$psql_stdin .= q[
SELECT 'OK';
];

ok(pump_until(
        $psql, $psql_timeout, \$psql_stderr, qr/FATAL:  terminating connection due to idle-in-transaction timeout/m), "expected FATAL message received");

print($psql_stdin . "\n");
print("psql stdout: ". $psql_stdout . "\npsql stderr: " . $psql_stderr. "\n");

ok(1);
$node->stop;

done_testing();
