use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 70;

my ($node, $port);

# Returns the filesystem path for the named relation.
#
# Assumes the test node is running
sub relation_filepath($$)
{
	my ($dbname, $relname) = @_;

	my $pgdata = $node->data_dir;
	my $rel = $node->safe_psql($dbname,
							   qq(SELECT pg_relation_filepath('$relname')));
	die "path not found for relation $relname" unless defined $rel;
	return "$pgdata/$rel";
}

# Returns the name of the toast relation associated with the named relation.
#
# Assumes the test node is running
sub relation_toast($$)
{
	my ($dbname, $relname) = @_;

	my $rel = $node->safe_psql($dbname, qq(
		SELECT ct.relname
			FROM pg_catalog.pg_class cr, pg_catalog.pg_class ct
			WHERE cr.oid = '$relname'::regclass
			  AND cr.reltoastrelid = ct.oid
			));
	return undef unless defined $rel;
	return "pg_toast.$rel";
}

# Stops the test node, corrupts the first page of the named relation, and
# restarts the node.
#
# Assumes the test node is running.
sub corrupt_first_page($$)
{
	my ($dbname, $relname) = @_;
	my $relpath = relation_filepath($dbname, $relname);

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
sub remove_relation_file($$)
{
	my ($dbname, $relname) = @_;
	my $relpath = relation_filepath($dbname, $relname);

	$node->stop();
	unlink($relpath);
	$node->start;
}

# Stops the test node, unlinks the file from the filesystem that backs the
# toast table (if any) corresponding to the given main table relation, and
# restarts the node.
#
# Assumes the test node is running
sub remove_toast_file($$)
{
	my ($dbname, $relname) = @_;
	my $toastname = relation_toast($dbname, $relname);
	remove_relation_file($dbname, $toastname) if ($toastname);
}

# Test set-up
$node = get_new_node('test');
$node->init;
$node->start;
$port = $node->port;

for my $dbname (qw(db1 db2 db3))
{
	# Create the database
	$node->safe_psql('postgres', qq(CREATE DATABASE $dbname));

	# Load the amcheck extension, upon which pg_amcheck depends
	$node->safe_psql($dbname, q(CREATE EXTENSION amcheck));

	# Create schemas, tables and indexes in five separate
	# schemas.  The schemas are all identical to start, but
	# we will corrupt them differently later.
	#
	for my $schema (qw(s1 s2 s3 s4 s5))
	{
		$node->safe_psql($dbname, qq(
			CREATE SCHEMA $schema;
			CREATE SEQUENCE $schema.seq1;
			CREATE SEQUENCE $schema.seq2;
			CREATE TABLE $schema.t1 (
				i INTEGER,
				b BOX,
				ia int4[],
				ir int4range,
				t TEXT
			);
			CREATE TABLE $schema.t2 (
				i INTEGER,
				b BOX,
				ia int4[],
				ir int4range,
				t TEXT
			);
			CREATE VIEW $schema.t2_view AS (
				SELECT i*2, t FROM $schema.t2
			);
			ALTER TABLE $schema.t2
				ALTER COLUMN t
				SET STORAGE EXTERNAL;

			INSERT INTO $schema.t1 (i, b, ia, ir, t)
				(SELECT gs::INTEGER AS i,
						box(point(gs,gs+5),point(gs*2,gs*3)) AS b,
						array[gs, gs + 1]::int4[] AS ia,
						int4range(gs, gs+100) AS ir,
						repeat('foo', gs) AS t
					 FROM generate_series(1,10000,3000) AS gs);

			INSERT INTO $schema.t2 (i, b, ia, ir, t)
				(SELECT gs::INTEGER AS i,
						box(point(gs,gs+5),point(gs*2,gs*3)) AS b,
						array[gs, gs + 1]::int4[] AS ia,
						int4range(gs, gs+100) AS ir,
						repeat('foo', gs) AS t
					 FROM generate_series(1,10000,3000) AS gs);

			CREATE MATERIALIZED VIEW $schema.t1_mv AS SELECT * FROM $schema.t1;
			CREATE MATERIALIZED VIEW $schema.t2_mv AS SELECT * FROM $schema.t2;

			create table $schema.p1 (a int, b int) PARTITION BY list (a);
			create table $schema.p2 (a int, b int) PARTITION BY list (a);

			create table $schema.p1_1 partition of $schema.p1 for values in (1, 2, 3);
			create table $schema.p1_2 partition of $schema.p1 for values in (4, 5, 6);
			create table $schema.p2_1 partition of $schema.p2 for values in (1, 2, 3);
			create table $schema.p2_2 partition of $schema.p2 for values in (4, 5, 6);

			CREATE INDEX t1_btree ON $schema.t1 USING BTREE (i);
			CREATE INDEX t2_btree ON $schema.t2 USING BTREE (i);

			CREATE INDEX t1_hash ON $schema.t1 USING HASH (i);
			CREATE INDEX t2_hash ON $schema.t2 USING HASH (i);

			CREATE INDEX t1_brin ON $schema.t1 USING BRIN (i);
			CREATE INDEX t2_brin ON $schema.t2 USING BRIN (i);

			CREATE INDEX t1_gist ON $schema.t1 USING GIST (b);
			CREATE INDEX t2_gist ON $schema.t2 USING GIST (b);

			CREATE INDEX t1_gin ON $schema.t1 USING GIN (ia);
			CREATE INDEX t2_gin ON $schema.t2 USING GIN (ia);

			CREATE INDEX t1_spgist ON $schema.t1 USING SPGIST (ir);
			CREATE INDEX t2_spgist ON $schema.t2 USING SPGIST (ir);
		));
	}
}

# Database 'db1' corruptions
#

# Corrupt indexes in schema "s1"
remove_relation_file('db1', 's1.t1_btree');
corrupt_first_page('db1', 's1.t2_btree');

# Corrupt tables in schema "s2"
remove_relation_file('db1', 's2.t1');
corrupt_first_page('db1', 's2.t2');

# Corrupt tables, partitions, matviews, and btrees in schema "s3"
remove_relation_file('db1', 's3.t1');
corrupt_first_page('db1', 's3.t2');

remove_relation_file('db1', 's3.t1_mv');
remove_relation_file('db1', 's3.p1_1');

corrupt_first_page('db1', 's3.t2_mv');
corrupt_first_page('db1', 's3.p2_1');

remove_relation_file('db1', 's3.t1_btree');
corrupt_first_page('db1', 's3.t2_btree');

# Corrupt toast table, partitions, and materialized views in schema "s4"
remove_toast_file('db1', 's4.t2');

# Corrupt all other object types in schema "s5".  We don't have amcheck support
# for these types, but we check that their corruption does not trigger any
# errors in pg_amcheck
remove_relation_file('db1', 's5.seq1');
remove_relation_file('db1', 's5.t1_hash');
remove_relation_file('db1', 's5.t1_gist');
remove_relation_file('db1', 's5.t1_gin');
remove_relation_file('db1', 's5.t1_brin');
remove_relation_file('db1', 's5.t1_spgist');

corrupt_first_page('db1', 's5.seq2');
corrupt_first_page('db1', 's5.t2_hash');
corrupt_first_page('db1', 's5.t2_gist');
corrupt_first_page('db1', 's5.t2_gin');
corrupt_first_page('db1', 's5.t2_brin');
corrupt_first_page('db1', 's5.t2_spgist');


# Database 'db2' corruptions
#
remove_relation_file('db2', 's1.t1');
remove_relation_file('db2', 's1.t1_btree');


# Leave 'db3' uncorrupted
#


# Standard first arguments to TestLib functions
my @cmd = ('pg_amcheck', '--quiet', '-p', $port);

# The pg_amcheck command itself should return a success exit status, even
# though tables and indexes are corrupt.  An error code returned would mean the
# pg_amcheck command itself failed, for example because a connection to the
# database could not be established.
#
# For these checks, we're ignoring any corruption reported and focusing
# exclusively on the exit code from pg_amcheck.
#
$node->command_ok(
	[ @cmd,, 'db1' ],
	'pg_amcheck all schemas, tables and indexes in database db1');

$node->command_ok(
	[ @cmd,, 'db1', 'db2', 'db3' ],
	'pg_amcheck all schemas, tables and indexes in databases db1, db2 and db3');

$node->command_ok(
	[ @cmd, '--all' ],
	'pg_amcheck all schemas, tables and indexes in all databases');

$node->command_ok(
	[ @cmd, 'db1', '-s', 's1' ],
	'pg_amcheck all objects in schema s1');

$node->command_ok(
	[ @cmd, 'db1', '-r', 's*.t1' ],
	'pg_amcheck all tables named t1 and their indexes');

$node->command_ok(
	[ @cmd, 'db1', '-i', 'i*.idx', '-i', 'idx.i*' ],
	'pg_amcheck all indexes with qualified names matching /i*.idx/ or /idx.i*/');

$node->command_ok(
	[ @cmd, '--no-dependents', 'db1', '-r', 's*.t1' ],
	'pg_amcheck all tables with qualified names matching /s*.t1/');

$node->command_ok(
	[ @cmd, '--no-dependents', 'db1', '-t', 's*.t1', '-t', 'foo*.bar*' ],
	'pg_amcheck all tables with qualified names matching /s*.t1/ or /foo*.bar*/');

$node->command_ok(
	[ @cmd, 'db1', '-T', 't1' ],
	'pg_amcheck everything except tables named t1');

$node->command_ok(
	[ @cmd, 'db1', '-S', 's1', '-R', 't1' ],
	'pg_amcheck everything not named t1 nor in schema s1');

$node->command_ok(
	[ @cmd, 'db1', '-t', '*.*.*' ],
	'pg_amcheck all tables across all databases and schemas');

$node->command_ok(
	[ @cmd, 'db1', '-t', '*.*.t1' ],
	'pg_amcheck all tables named t1 across all databases and schemas');

$node->command_ok(
	[ @cmd, 'db1', '-t', '*.s1.*' ],
	'pg_amcheck all tables across all databases in schemas named s1');

$node->command_ok(
	[ @cmd, 'db1', '-t', 'db2.*.*' ],
	'pg_amcheck all tables across all schemas in database db2');

$node->command_ok(
	[ @cmd, 'db1', '-t', 'db2.*.*', '-t', 'db3.*.*' ],
	'pg_amcheck all tables across all schemas in databases db2 and db3');

# Scans of indexes in s1 should detect the specific corruption that we created
# above.  For missing relation forks, we know what the error message looks
# like.  For corrupted index pages, the error might vary depending on how the
# page was formatted on disk, including variations due to alignment differences
# between platforms, so we accept any non-empty error message.
#
$node->command_like(
	[ @cmd, '--all', '-s', 's1', '-i', 't1_btree' ],
	qr/index "t1_btree" lacks a main relation fork/,
	'pg_amcheck index s1.t1_btree reports missing main relation fork');

$node->command_like(
	[ @cmd, 'db1', '-s', 's1', '-i', 't2_btree' ],
	qr/.+/,			# Any non-empty error message is acceptable
	'pg_amcheck index s1.s2 reports index corruption');

# Checking db1.s1 should show no corruptions if indexes are excluded
$node->command_like(
	[ @cmd, 'db1', '-s', 's1', '--exclude-indexes' ],
	qr/^$/,
	'pg_amcheck of db1.s1 excluding indexes');

# But checking across all databases in schema s1 should show corruptions
# messages for tables in db2
$node->command_like(
	[ @cmd, '--all', '-s', 's1', '--exclude-indexes' ],
	qr/could not open file/,
	'pg_amcheck of schema s1 across all databases but excluding indexes');

# Checking across a list of databases should also work
$node->command_like(
	[ @cmd, '-d', 'db2', '-d', 'db1', '-s', 's1', '--exclude-indexes' ],
	qr/could not open file/,
	'pg_amcheck of schema s1 across db1 and db2 but excluding indexes');

# In schema s3, the tables and indexes are both corrupt.  We should see
# corruption messages on stdout, nothing on stderr, and an exit
# status of zero.
#
$node->command_checks_all(
	[ @cmd, 'db1', '-s', 's3' ],
	0,
	[ qr/index "t1_btree" lacks a main relation fork/,
	  qr/could not open file/ ],
	[ qr/^$/ ],
	'pg_amcheck schema s3 reports table and index errors');

# In schema s2, only tables are corrupt.  Check that table corruption is
# reported as expected.
#
$node->command_like(
	[ @cmd, 'db1', '-s', 's2', '-t', 't1' ],
	qr/could not open file/,
	'pg_amcheck in schema s2 reports table corruption');

$node->command_like(
	[ @cmd, 'db1', '-s', 's2', '-t', 't2' ],
	qr/.+/,			# Any non-empty error message is acceptable
	'pg_amcheck in schema s2 reports table corruption');

# In schema s4, only toast tables are corrupt.  Check that under default
# options the toast corruption is reported, but when excluding toast we get no
# error reports.
$node->command_like(
	[ @cmd, 'db1', '-s', 's4' ],
	qr/could not open file/,
	'pg_amcheck in schema s4 reports toast corruption');

$node->command_like(
	[ @cmd, '--exclude-toast', '--exclude-toast-pointers', 'db1', '-s', 's4' ],
	qr/^$/,			# Empty
	'pg_amcheck in schema s4 excluding toast reports no corruption');

# Check that no corruption is reported in schema s5
$node->command_like(
	[ @cmd, 'db1', '-s', 's5' ],
	qr/^$/,			# Empty
	'pg_amcheck over schema s5 reports no corruption');

$node->command_like(
	[ @cmd, 'db1', '-s', 's1', '-I', 't1_btree', '-I', 't2_btree' ],
	qr/^$/,			# Empty
	'pg_amcheck over schema s1 with corrupt indexes excluded reports no corruption');

$node->command_like(
	[ @cmd, 'db1', '-s', 's1', '--exclude-indexes' ],
	qr/^$/,			# Empty
	'pg_amcheck over schema s1 with all indexes excluded reports no corruption');

$node->command_like(
	[ @cmd, 'db1', '-s', 's2', '-T', 't1', '-T', 't2' ],
	qr/^$/,			# Empty
	'pg_amcheck over schema s2 with corrupt tables excluded reports no corruption');

# Check errors about bad block range command line arguments.  We use schema s5
# to avoid getting messages about corrupt tables or indexes.
command_fails_like(
	[ @cmd, 'db1', '-s', 's5', '--startblock', 'junk' ],
	qr/relation starting block argument contains garbage characters/,
	'pg_amcheck rejects garbage startblock');

command_fails_like(
	[ @cmd, 'db1', '-s', 's5', '--endblock', '1234junk' ],
	qr/relation ending block argument contains garbage characters/,
	'pg_amcheck rejects garbage endblock');

command_fails_like(
	[ @cmd, 'db1', '-s', 's5', '--startblock', '5', '--endblock', '4' ],
	qr/relation ending block argument precedes starting block argument/,
	'pg_amcheck rejects invalid block range');

# Check bt_index_parent_check alternates.  We don't create any index corruption
# that would behave differently under these modes, so just smoke test that the
# arguments are handled sensibly.

$node->command_like(
	[ @cmd, 'db1', '-s', 's1', '-i', 't1_btree', '--parent-check' ],
	qr/index "t1_btree" lacks a main relation fork/,
	'pg_amcheck smoke test --parent-check');

$node->command_like(
	[ @cmd, 'db1', '-s', 's1', '-i', 't1_btree', '--heapallindexed', '--rootdescend' ],
	qr/index "t1_btree" lacks a main relation fork/,
	'pg_amcheck smoke test --heapallindexed --rootdescend');
