# Test postgres_fdw reaction on target server disconnection

use strict;
use warnings;

use PostgreSQL::Test::Utils;
use Test::More tests => 1;
use PostgreSQL::Test::Cluster;

use Time::HiRes qw(time usleep);
use DateTime;

use threads;
use threads::shared;

sub restart_thread {
	my @args = @_;
	my ($server) = @args;
	$SIG{'KILL'} = sub { threads->exit(); };
	sleep(3);
	for (my $i = 0; $i < 100; $i++) {
		sleep(5);
		$server->restart();
	}
}

my $foreign = PostgreSQL::Test::Cluster->new('foreign');
$foreign->init();

$foreign->append_conf(
	'postgresql.conf', qq{
log_min_messages = DEBUG1
log_min_error_statement = log
log_connections = on
log_disconnections = on
log_line_prefix = '%m|%u|%d|%c|'
log_statement = 'all'
	});
$foreign->start;
my $foreign_port = $foreign->port;

my $local = PostgreSQL::Test::Cluster->new('local');
$local->init();

$local->append_conf(
	'postgresql.conf', qq{
log_min_messages = DEBUG1
log_min_error_statement = log
log_connections = on
log_disconnections = on
log_line_prefix = '%m|%u|%d|%c|'
log_statement = 'all'
	});
$local->start();

$foreign->safe_psql('postgres', qq{
CREATE TABLE large(a int, t text);
INSERT INTO large SELECT x, rpad(x::text, 100) FROM generate_series(0, 999999) x;
});


$local->safe_psql('postgres', qq{
CREATE EXTENSION postgres_fdw;
CREATE SERVER fpg FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname 'postgres', port '$foreign_port');
CREATE USER MAPPING FOR CURRENT_USER SERVER fpg;
CREATE FUNCTION fx2(i integer) RETURNS int AS 'begin return i * 2; end;' LANGUAGE plpgsql;
});

my $outputdir = $PostgreSQL::Test::Utils::tmp_check;

$local->safe_psql('postgres', 'IMPORT FOREIGN SCHEMA public FROM SERVER fpg INTO public;');
print($local->psql('postgres', 'EXPLAIN (VERBOSE) SELECT * FROM large WHERE a = fx2(a)') . "\n");
my $thread = threads->create('restart_thread', $foreign);
$ENV{PGHOST} = $local->host;
$ENV{PGPORT} = $local->port;
$ENV{PGCTLTIMEOUT} = 180;
for (my $i = 1; $i <= 50; $i++) {
	diag(" executing query ($i)...");
# Avoid using IPC::Run due to it's own quirks
#	my ($ret, $stdout, $stderr) = $local->psql('postgres', 'SELECT * FROM large WHERE a = fx2(a)');
	my $ret = system("psql postgres -c \"SELECT $i i, * FROM large WHERE a = fx2(a)\" >>\"$outputdir/psql.log\" 2>&1");
	diag(" result: \t$ret");
}
$thread->kill('KILL')->detach;
sleep(2);
$foreign->_update_pid(1);
ok(1);
