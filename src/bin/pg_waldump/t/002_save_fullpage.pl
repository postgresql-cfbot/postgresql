
# Copyright (c) 2022, PostgreSQL Global Development Group

use strict;
use warnings;
use File::Basename;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::RecursiveCopy;
use PostgreSQL::Test::Utils;
use Test::More;

my ($blocksize, $walfile_name);

# routine to extract the LSN from the given block structure
sub get_block_info
{
	my $path = shift;
	my $block;

	open my $fh, '<', $path or die "couldn't open file: $path\n";
	die "couldn't read full block\n" if $blocksize != read $fh, $block, $blocksize;
	my ($lsn_hi, $lsn_lo) = unpack('VV', $block);

	$lsn_hi = sprintf('%08X', $lsn_hi);
	$lsn_lo = sprintf('%08X', $lsn_lo);

	return ($lsn_hi, $lsn_lo);
}

# Set umask so test directories and files are created with default permissions
umask(0077);

my $node =  PostgreSQL::Test::Cluster->new('primary');
$node->init(extra => ['-k'], allows_streaming => 1);
$node->start;

# Sanity checks for command line options.
$node->command_fails(
	[ 'pg_waldump', '--save-fpi' ],
	'--save-fpi fails without path');

# generate data/wal to examine that will have FPIs in them
$node->safe_psql('postgres', <<EOF);
SELECT 'init' FROM pg_create_physical_replication_slot('regress_pg_waldump_slot', true, false);
CREATE TABLE test_table AS SELECT generate_series(1,100) a;
CHECKPOINT; -- required to force FPI for next writes
UPDATE test_table SET a = a + 1;
EOF

($walfile_name, $blocksize) = split '\|' => $node->safe_psql('postgres',"SELECT pg_walfile_name(pg_switch_wal()), current_setting('block_size')");

# get the relation node, etc for the new table
my $relation = $node->safe_psql('postgres',
	q{SELECT format('%s/%s/%s', CASE WHEN reltablespace = 0 THEN dattablespace ELSE reltablespace END, pg_database.oid, pg_relation_filenode(pg_class.oid)) FROM pg_class, pg_database WHERE relname = 'test_table' AND datname = current_database()}
);

diag $relation;

my $walfile = $node->basedir . '/pgdata/pg_wal/' . $walfile_name;
my $tmp_folder = PostgreSQL::Test::Utils::tempdir;

diag "using walfile: $walfile";

ok(-f $walfile, "Got a WAL file");

$node->command_ok(['pg_waldump', '--save-fpi', "$tmp_folder/raw", '--relation', $relation, $walfile]);

my $file_re =
  qr/^([0-9A-F]{8})-([0-9A-F]{8})[.][0-9]+[.][0-9]+[.][0-9]+[.][0-9]+(?:_vm|_init|_fsm|_main)?$/;

my %files;

# verify filename formats matches w/--save-fpi
for my $fullpath (glob "$tmp_folder/raw/*")
{
	my $file = File::Basename::basename($fullpath);

	like($file, $file_re, "verify filename format for file $file");

	# save filename for later verification
	$files{$file}++;

	my ($hi_lsn_fn, $lo_lsn_fn) = ($file =~ $file_re);
	my ($hi_lsn_bk, $lo_lsn_bk) = get_block_info($fullpath);

	# verify the lsn in the block comes before the file's lsn
	ok( $hi_lsn_fn . $lo_lsn_fn gt $hi_lsn_bk . $lo_lsn_bk,
		'verify file-based LSN precedes block-based one');
}

# validate that we ended up with some FPIs saved
ok(keys %files > 0, 'verify we processed some files');

$node->safe_psql('postgres', "SELECT pg_drop_replication_slot('regress_pg_waldump_slot')");

done_testing();
