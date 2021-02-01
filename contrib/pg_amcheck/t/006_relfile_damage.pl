use strict;
use warnings;

use TestLib;
use Test::More tests => 27;
use PostgresNode;

my ($node, $port);

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

# Test set-up
$node = get_new_node('test');
$node->init;
$node->start;
$port = $node->port;

# Load the amcheck extension, upon which pg_amcheck depends
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));

# Create a table with a btree index.  Use a fillfactor for the table and index
# that will allow some fraction of updates to be on the original pages and some
# on new pages.
#
$node->safe_psql('postgres', qq(
create schema t;
create table t.t1 (id integer, t text) with (fillfactor=75);
alter table t.t1 alter column t set storage external;
insert into t.t1 select gs, repeat('x',gs) from generate_series(9990,10000) gs;
create index t1_idx on t.t1 (id) with (fillfactor=75);
));

my $toastrel = relation_toast('postgres', 't.t1');

# Flush relation files to disk and take snapshots of the toast and index
#
$node->restart;
$node->take_relfile_snapshot_minimal('postgres', 'idx', 't.t1_idx');
$node->take_relfile_snapshot_minimal('postgres', 'toast', $toastrel);

# Insert new data into the table and index
#
$node->safe_psql('postgres', qq(
insert into t.t1 select gs, repeat('y',gs) from generate_series(10001,10100) gs;
));

# Revert index.  The reverted snapshot file is not corrupt, but it also
# does not match the current contents of the table.
#
$node->stop;
$node->revert_to_snapshot('idx');

# Restart the node and check table and index with varying options.
#
$node->start;

# Checks which do not reconcile the index and table via --heapallindexed will
# not notice any problems
#
$node->command_like(
	[ 'pg_amcheck', '--quiet', '-p', $port, '-r', 'postgres.t.*' ],
	qr/^$/,
	'pg_amcheck reverted index at default checking level');

$node->command_like(
	[ 'pg_amcheck', '--quiet', '-p', $port, '-r', 'postgres.t.*' ],
	qr/^$/,
	'pg_amcheck reverted index at default checking level');

$node->command_like(
	[ 'pg_amcheck', '--quiet', '-p', $port, '-r', 'postgres.t.*', '--parent-check' ],
	qr/^$/,
	'pg_amcheck with torn pages with --parent-check');

$node->command_like(
	[ 'pg_amcheck', '--quiet', '-p', $port, '-r', 'postgres.t.*', '--rootdescend' ],
	qr/^$/,
	'pg_amcheck with torn pages with --rootdescend');

# Checks which do reconcile the index and table via --heapallindexed will
# notice the mismatch in their contents
#
$node->command_like(
	[ 'pg_amcheck', '--quiet', '-p', $port, '-r', 'postgres.t.*', '--heapallindexed' ],
	qr/heap tuple .* from table "t1" lacks matching index tuple within index "t1_idx"/,
	'pg_amcheck with torn pages with --heapallindexed');

$node->command_like(
	[ 'pg_amcheck', '--quiet', '-p', $port, '-r', 'postgres.t.*', '--heapallindexed', '--rootdescend' ],
	qr/heap tuple .* from table "t1" lacks matching index tuple within index "t1_idx"/,
	'pg_amcheck with torn pages with --heapallindexed --rootdescend');

# Revert the toast.  The reverted toast table is not corrupt, but it does not
# have entries for all toast pointers in the main table
#
$node->stop;
$node->revert_to_snapshot('toast');

# Restart the node and check table and toast with varying options.
#
$node->start;

$node->command_like(
	[ 'pg_amcheck', '--quiet', '-p', $port, '-r', $toastrel ],
	qr/^$/,
	'pg_amcheck reverted toast table');

$node->command_like(
	[ 'pg_amcheck', '--quiet', '-p', $port, '-r', 'postgres.t.*', '--exclude-toast-pointers' ],
	qr/^$/,
	'pg_amcheck with reverted toast using --exclude-toast-pointers');

$node->command_like(
	[ 'pg_amcheck', '--quiet', '-p', $port, '-r', 'postgres.t.*' ],
	qr/ERROR:  could not read block/,
	'pg_amcheck with reverted toast and default checking');
