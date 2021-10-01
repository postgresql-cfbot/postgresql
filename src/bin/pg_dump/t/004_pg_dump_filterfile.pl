use strict;
use warnings;

use Config;
use PostgresNode;
use TestLib;
use Test::More tests => 29;

my $tempdir       = TestLib::tempdir;
my $inputfile;


my $node = PostgresNode->new('main');
my $port = $node->port;
my $backupdir = $node->backup_dir;
my $plainfile = "$backupdir/plain.sql";

$node->init;
$node->start;

# Generate test objects
$node->safe_psql('postgres', 'CREATE FOREIGN DATA WRAPPER dummy;');
$node->safe_psql('postgres', 'CREATE SERVER dummyserver FOREIGN DATA WRAPPER dummy;');

$node->safe_psql('postgres', "CREATE TABLE table_one(a varchar)");
$node->safe_psql('postgres', "CREATE TABLE table_two(a varchar)");
$node->safe_psql('postgres', "CREATE TABLE table_three(a varchar)");
$node->safe_psql('postgres', "CREATE TABLE \"strange aaa
name\"(a varchar)");
$node->safe_psql('postgres', "CREATE TABLE \"
t
t
\"(a int)");

$node->safe_psql('postgres', "INSERT INTO table_one VALUES('*** TABLE ONE ***')");
$node->safe_psql('postgres', "INSERT INTO table_two VALUES('*** TABLE TWO ***')");
$node->safe_psql('postgres', "INSERT INTO table_three VALUES('*** TABLE THREE ***')");

open $inputfile, '>', "$tempdir/inputfile.txt"
	or die "unable to open filterfile for writing";
print $inputfile "  include   table table_one    #comment\n";
print $inputfile "include table table_two\n";
print $inputfile "# skip this line\n";
print $inputfile "\n";
print $inputfile "exclude data table_one\n";
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

open $inputfile, '>', "$tempdir/inputfile.txt"
	or die "unable to open filterfile for writing";
print $inputfile "exclude table table_one\n";
close $inputfile;

command_ok(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	"dump tables with filter");

$dump = slurp_file($plainfile);

ok($dump !~ qr/^CREATE TABLE public.table_one/m, "table one not dumped");
ok($dump =~ qr/^CREATE TABLE public.table_two/m, "dumped table two");
ok($dump =~ qr/^CREATE TABLE public.table_three/m, "dumped table three");

open $inputfile, '>', "$tempdir/inputfile.txt"
	or die "unable to open filterfile for writing";
print $inputfile "include table \"strange aaa
name\"";
close $inputfile;

command_ok(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	"dump tables with filter");

$dump = slurp_file($plainfile);

ok($dump =~ qr/^CREATE TABLE public.\"strange aaa/m, "dump table with new line in name");

open $inputfile, '>', "$tempdir/inputfile.txt"
	or die "unable to open filterfile for writing";
print $inputfile "exclude table \"strange aaa\\nname\"";
close $inputfile;

command_ok(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	"dump tables with filter");

$dump = slurp_file($plainfile);

ok($dump !~ qr/^CREATE TABLE public.\"strange aaa/m, "dump table with new line in name");

open $inputfile, '>', "$tempdir/inputfile.txt"
	or die "unable to open filterfile for writing";
print $inputfile "exclude schema public\n";
close $inputfile;

command_ok(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	"dump tables with filter");

$dump = slurp_file($plainfile);

ok($dump !~ qr/^CREATE TABLE/m, "no table dumped");

open $inputfile, '>', "$tempdir/inputfile.txt"
	or die "unable to open filterfile for writing";
print $inputfile "include table \"
t
t
\"";
close $inputfile;

command_ok(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	"dump tables with filter");

$dump = slurp_file($plainfile);

ok($dump =~ qr/^CREATE TABLE public.\"\nt\nt\n\" \($/ms, "dump table with multiline strange name");

open $inputfile, '>', "$tempdir/inputfile.txt"
	or die "unable to open filterfile for writing";
print $inputfile "include table \"\\nt\\nt\\n\"";
close $inputfile;

command_ok(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	"dump tables with filter");

$dump = slurp_file($plainfile);

ok($dump =~ qr/^CREATE TABLE public.\"\nt\nt\n\" \($/ms, "dump table with multiline strange name");

#########################################
# Test foreign_data

open $inputfile, '>', "$tempdir/inputfile.txt"
	or die "unable to open filterfile for writing";
print $inputfile "include foreign_data doesnt_exists\n";
close $inputfile;

command_fails_like(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	qr/pg_dump: error: no matching foreign servers were found for pattern/,
	"dump foreign server");

open $inputfile, '>', "$tempdir/inputfile.txt"
	or die "unable to open filterfile for writing";
print $inputfile, "include foreign_data dummy*\n";
close $inputfile;

command_ok(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	"dump foreign_data with filter");

#########################################
# Test broken input format

open $inputfile, '>', "$tempdir/inputfile.txt"
	or die "unable to open filterfile for writing";
print $inputfile "k";
close $inputfile;

command_fails_like(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	qr/invalid keyword/,
	"broken format check");

open $inputfile, '>', "$tempdir/inputfile.txt"
	or die "unable to open filterfile for writing";
print $inputfile "include xxx";
close $inputfile;

command_fails_like(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	qr/invalid keyword/,
	"broken format check");

open $inputfile, '>', "$tempdir/inputfile.txt"
	or die "unable to open filterfile for writing";
print $inputfile "include table";
close $inputfile;

command_fails_like(
	[ "pg_dump", '-p', $port, "-f", $plainfile, "--filter=$tempdir/inputfile.txt", 'postgres' ],
	qr/missing object name/,
	"broken format check");
