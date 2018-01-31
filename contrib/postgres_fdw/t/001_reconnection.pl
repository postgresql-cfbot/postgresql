# Minimal test testing reconnection
use strict;
use warnings;
use PostgresNode;
use PostgresClient;
use TestLib;
use Test::More tests => 5;

# start a server
my $server = get_new_node('server');
$server->init();
$server->start;
my $session1 = $server->get_new_session('postgres', 'session1');
my $session2 = $server->get_new_session('postgres', 'session2');
my $dbname = $session1->db();
my $port = $session1->port();

ok (!$session1->exec_multi(
	"CREATE EXTENSION postgres_fdw;",
	"CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname \'$dbname\', port \'$port\');",
	"CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;",
	"CREATE TABLE lt1 (c1 int);",
	"INSERT INTO lt1 VALUES (1);",
	"CREATE FOREIGN TABLE ft1 (c1 int) SERVER loopback OPTIONS (table_name 'lt1');",
	"SET client_min_messages to DEBUG3;"),
	'setting up');

$session1->exec("BEGIN;");

my $result = $session1->exec("SELECT c1 FROM ft1 LIMIT 1;");

# check if the connection has been made
ok($session1->notice() =~ /DEBUG: *new postgres_fdw connection 0x[[:xdigit:]]+/,
   "creating new fdw connection");

# change server host
$session2->exec_multi(
	"ALTER SERVER loopback OPTIONS (ADD host 'hoge')",
	"ALTER SERVER loopback OPTIONS (DROP host)");

# and no more
$session1->clear_notice();
$result = $session1->exec("SELECT c1 FROM ft1 LIMIT 1;");
ok($session1->notice() !~ /DEBUG: *closing connection 0x[[:xdigit:]]+ for option changes to take effect/,
   'check if no disconnection happens within a transaction');

$session1->exec("COMMIT;");

# access to ft1 here causes reconnection
$session1->clear_notice();
$result = $session1->exec("SELECT c1 FROM ft1 LIMIT 1;");
ok($session1->notice() =~ /DEBUG: *closing connection 0x[[:xdigit:]]+ for option changes to take effect\nDEBUG: *new postgres_fdw connection 0x[[:xdigit:]]+/,
   'reconnection by option change happens after the end of the transactin');

# and no more
$session1->clear_notice();
$result = $session1->exec("SELECT c1 FROM ft1 LIMIT 1;");
ok($session1->notice() !~ /DEBUG: *closing connection 0x[[:xdigit:]]+ for option changes to take effect/,
    'no disconnection without option change');

$session1->finish;
$session2->finish;
$server->stop;
