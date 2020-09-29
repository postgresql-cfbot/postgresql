use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 39;

my ($node, $port);

# Returns the filesystem path for the named relation.
#
# Assumes the test node is running
sub relation_filepath($)
{
	my ($relname) = @_;

	my $pgdata = $node->data_dir;
	my $rel = $node->safe_psql('postgres',
							   qq(SELECT pg_relation_filepath('$relname')));
	die "path not found for relation $relname" unless defined $rel;
	return "$pgdata/$rel";
}

# Stops the test node, corrupts the first page of the named relation, and
# restarts the node.
#
# Assumes the node is running.
sub corrupt_first_page($)
{
	my ($relname) = @_;
	my $relpath = relation_filepath($relname);

	$node->stop;
	my $fh;
	open($fh, '+<', $relpath);
	binmode $fh;
	seek($fh, 32, 0);
	syswrite($fh, '\x77\x77\x77\x77', 500);
	close($fh);
	$node->start;
}

# Stops the test node, unlinks the file from the filesystem that backs the
# relation, and restarts the node.
#
# Assumes the test node is running
sub remove_relation_file($)
{
	my ($relname) = @_;
	my $relpath = relation_filepath($relname);

	$node->stop();
	unlink($relpath);
	$node->start;
}

# Test set-up
$node = get_new_node('test');
$node->init;
$node->start;
$port = $node->port;

# Load the amcheck extension, upon which pg_amcheck depends
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));

# Create schemas and tables for checking pg_amcheck's include
# and exclude schema and table command line options
$node->safe_psql('postgres', q(
-- We'll corrupt all indexes in s1
CREATE SCHEMA s1;
CREATE TABLE s1.t1 (a TEXT);
CREATE TABLE s1.t2 (a TEXT);
CREATE INDEX i1 ON s1.t1(a);
CREATE INDEX i2 ON s1.t2(a);
INSERT INTO s1.t1 (a) (SELECT gs::TEXT FROM generate_series(1,10000) AS gs);
INSERT INTO s1.t2 (a) (SELECT gs::TEXT FROM generate_series(1,10000) AS gs);

-- We'll corrupt all tables in s2
CREATE SCHEMA s2;
CREATE TABLE s2.t1 (a TEXT);
CREATE TABLE s2.t2 (a TEXT);
CREATE INDEX i1 ON s2.t1(a);
CREATE INDEX i2 ON s2.t2(a);
INSERT INTO s2.t1 (a) (SELECT gs::TEXT FROM generate_series(1,10000) AS gs);
INSERT INTO s2.t2 (a) (SELECT gs::TEXT FROM generate_series(1,10000) AS gs);

-- We'll corrupt all tables and indexes in s3
CREATE SCHEMA s3;
CREATE TABLE s3.t1 (a TEXT);
CREATE TABLE s3.t2 (a TEXT);
CREATE INDEX i1 ON s3.t1(a);
CREATE INDEX i2 ON s3.t2(a);
INSERT INTO s3.t1 (a) (SELECT gs::TEXT FROM generate_series(1,10000) AS gs);
INSERT INTO s3.t2 (a) (SELECT gs::TEXT FROM generate_series(1,10000) AS gs);

-- We'll leave everything in s4 uncorrupted
CREATE SCHEMA s4;
CREATE TABLE s4.t1 (a TEXT);
CREATE TABLE s4.t2 (a TEXT);
CREATE INDEX i1 ON s4.t1(a);
CREATE INDEX i2 ON s4.t2(a);
INSERT INTO s4.t1 (a) (SELECT gs::TEXT FROM generate_series(1,10000) AS gs);
INSERT INTO s4.t2 (a) (SELECT gs::TEXT FROM generate_series(1,10000) AS gs);
));

# Corrupt indexes in schema "s1"
remove_relation_file('s1.i1');
corrupt_first_page('s1.i2');

# Corrupt tables in schema "s2"
remove_relation_file('s2.t1');
corrupt_first_page('s2.t2');

# Corrupt tables and indexes in schema "s3"
remove_relation_file('s3.i1');
corrupt_first_page('s3.i2');
remove_relation_file('s3.t1');
corrupt_first_page('s3.t2');

# Leave schema "s4" alone


# The pg_amcheck command itself should return a success exit status, even
# though tables and indexes are corrupt.  An error code returned would mean the
# pg_amcheck command itself failed, for example because a connection to the
# database could not be established.
#
# For these checks, we're ignoring any corruption reported and focusing
# exclusively on the exit code from pg_amcheck.
#
$node->command_ok(
	[ 'pg_amcheck', '-x', '-p', $port, 'postgres' ],
	'pg_amcheck all schemas and tables');

$node->command_ok(
	[ 'pg_amcheck', '-p', $port, 'postgres' ],
	'pg_amcheck all schemas, tables and indexes');

$node->command_ok(
	[ 'pg_amcheck', '-p', $port, 'postgres', '-n', 's1' ],
	'pg_amcheck all objects in schema s1');

$node->command_ok(
	[ 'pg_amcheck', '-p', $port, 'postgres', '-n', 's*', '-t', 't1' ],
	'pg_amcheck all tables named t1 and their indexes');

$node->command_ok(
	[ 'pg_amcheck', '-x', '-p', $port, 'postgres', '-T', 't1' ],
	'pg_amcheck all tables not named t1');

$node->command_ok(
	[ 'pg_amcheck', '-x', '-p', $port, 'postgres', '-N', 's1', '-T', 't1' ],
	'pg_amcheck all tables not named t1 nor in schema s1');

# Scans of indexes in s1 should detect the specific corruption that we created
# above.  For missing relation forks, we know what the error message looks
# like.  For corrupted index pages, the error might vary depending on how the
# page was formatted on disk, including variations due to alignment differences
# between platforms, so we accept any non-empty error message.
#
$node->command_like(
	[ 'pg_amcheck', '-x', '-p', $port, 'postgres', '-n', 's1', '-i', 'i1' ],
	qr/index "i1" lacks a main relation fork/,
	'pg_amcheck index s1.i1 reports missing main relation fork');

$node->command_like(
	[ 'pg_amcheck', '-x', '-p', $port, 'postgres', '-n', 's1', '-i', 'i2' ],
	qr/.+/,			# Any non-empty error message is acceptable
	'pg_amcheck index s1.s2 reports index corruption');


# In schema s3, the tables and indexes are both corrupt.  Ordinarily, checking
# of indexes will not be performed for corrupt tables, but the --check-corrupt
# option (-c) forces the indexes to also be checked.
#
$node->command_like(
	[ 'pg_amcheck', '-x', '-c', '-p', $port, 'postgres', '-n', 's3', '-i', 'i1' ],
	qr/index "i1" lacks a main relation fork/,
	'pg_amcheck index s3.i1 reports missing main relation fork');

$node->command_like(
	[ 'pg_amcheck', '-x', '-c', '-p', $port, 'postgres', '-n', 's3', '-i', 'i2' ],
	qr/.+/,			# Any non-empty error message is acceptable
	'pg_amcheck index s3.s2 reports index corruption');


# Check that '-x' and '-X' work as expected.  Since only index corruption
# (and not table corruption) exists in s1, '-X' should give no errors, and
# '-x' should give errors about index corruption.
#
$node->command_like(
	[ 'pg_amcheck', '-x', '-p', $port, 'postgres', '-n', 's1' ],
	qr/.+/,			# Any non-empty error message is acceptable
	'pg_amcheck over tables and indexes in schema s1 reports corruption');

$node->command_like(
	[ 'pg_amcheck', '-X', '-p', $port, 'postgres', '-n', 's1' ],
	qr/^$/,			# Empty
	'pg_amcheck over only tables in schema s1 reports no corruption');


# Check that table corruption is reported as expected, with or without
# index checking
#
$node->command_like(
	[ 'pg_amcheck', '-x', '-p', $port, 'postgres', '-n', 's2' ],
	qr/could not open file/,
	'pg_amcheck over tables in schema s2 reports table corruption');

$node->command_like(
	[ 'pg_amcheck', '-p', $port, 'postgres', '-n', 's2' ],
	qr/could not open file/,
	'pg_amcheck over tables and indexes in schema s2 reports table corruption');

# Check that no corruption is reported in schema s4
$node->command_like(
	[ 'pg_amcheck', '-p', $port, 'postgres', '-n', 's4' ],
	qr/^$/,			# Empty
	'pg_amcheck over schema s4 reports no corruption');

# Check that no corruption is reported if we exclude corrupt schemas
$node->command_like(
	[ 'pg_amcheck', '-p', $port, 'postgres', '-N', 's1', '-N', 's2', '-N', 's3' ],
	qr/^$/,			# Empty
	'pg_amcheck excluding corrupt schemas reports no corruption');

# Check that no corruption is reported if we exclude corrupt tables
$node->command_like(
	[ 'pg_amcheck', '-p', $port, 'postgres', '-T', 't1', '-T', 't2' ],
	qr/^$/,			# Empty
	'pg_amcheck excluding corrupt tables reports no corruption');
