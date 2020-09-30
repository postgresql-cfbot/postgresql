use strict;
use warnings;

use Config;
use PostgresNode;
use TestLib;
use Test::More tests => 22;

my $tempdir       = TestLib::tempdir;
my $inputfile;


my $node = get_new_node('main');
my $port = $node->port;
my $backupdir = $node->backup_dir;
my $plainfile = "$backupdir/plain.sql";

$node->init;
$node->start;

$node->safe_psql('postgres', "CREATE TABLE table_one(a varchar)");
$node->safe_psql('postgres', "CREATE TABLE table_two(a varchar)");
$node->safe_psql('postgres', "CREATE TABLE table_three(a varchar)");
$node->safe_psql('postgres', "INSERT INTO table_one VALUES('*** TABLE ONE ***')");
$node->safe_psql('postgres', "INSERT INTO table_two VALUES('*** TABLE TWO ***')");
$node->safe_psql('postgres', "INSERT INTO table_three VALUES('*** TABLE THREE ***')");

open $inputfile, '>', "$tempdir/inputfile.txt";

print $inputfile "+t table_one\n";
print $inputfile "+t table_two\n";
print $inputfile "# skip this line\n";
print $inputfile "\n";
print $inputfile "-d table_one\n";
close $inputfile;

my ($cmd, $stdout, $stderr, $result);
command_ok(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	"dump tables with filter");

my $dump = slurp_file($plainfile);

ok($dump =~ qr/^CREATE TABLE public.table_one/m, "dumped table one");
ok($dump =~ qr/^CREATE TABLE public.table_two/m, "dumped table two");
ok($dump !~ qr/^CREATE TABLE public.table_three/m, "table three not dumped");
ok($dump !~ qr/^COPY public.table_one/m, "content of table one is not included");
ok($dump =~ qr/^COPY public.table_two/m, "content of table two is included");

open $inputfile, '>', "$tempdir/inputfile.txt";

print $inputfile "-t table_one\n";
close $inputfile;

command_ok(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	"dump tables with filter");

$dump = slurp_file($plainfile);

ok($dump !~ qr/^CREATE TABLE public.table_one/m, "table one not dumped");
ok($dump =~ qr/^CREATE TABLE public.table_two/m, "dumped table two");
ok($dump =~ qr/^CREATE TABLE public.table_three/m, "dumped table three");

open $inputfile, '>', "$tempdir/inputfile.txt";

print $inputfile "-n public\n";
close $inputfile;

command_ok(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	"dump tables with filter");

$dump = slurp_file($plainfile);

ok($dump !~ qr/^CREATE TABLE/m, "no table dumped");

#########################################
# For test of +f option we need created foreign server or accept
# fail and check error

open $inputfile, '>', "$tempdir/inputfile.txt";

print $inputfile "+f doesnt_exists\n";
close $inputfile;

command_fails_like(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	qr/pg_dump: error: no matching foreign servers were found for pattern/,
	"dump foreign server");

#########################################
# Test broken input format

open $inputfile, '>', "$tempdir/inputfile.txt";
print $inputfile "k";
close $inputfile;

command_fails_like(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	qr/pg_dump: error: invalid format of filter file/,
	"broken format check");

open $inputfile, '>', "$tempdir/inputfile.txt";
print $inputfile "+";
close $inputfile;

command_fails_like(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	qr/pg_dump: error: invalid format of filter file/,
	"broken format check");

open $inputfile, '>', "$tempdir/inputfile.txt";
print $inputfile "+d sometable";
close $inputfile;

command_fails_like(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	qr/include filter is not supported for this type of object/,
	"broken format check");

open $inputfile, '>', "$tempdir/inputfile.txt";
print $inputfile "-f someforeignserver";
close $inputfile;

command_fails_like(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	qr/exclude filter is not supported for this type of object/,
	"broken format check");
