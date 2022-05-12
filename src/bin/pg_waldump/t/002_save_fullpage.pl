
# Copyright (c) 2022, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;
use File::Basename;

# routine to extract the LSN and checksum from the given block structure
sub get_block_info
{
	my $path = shift;
	my $block;

	open my $fh, '<', $path or die "couldn't open file: $path\n";
	die "couldn't read full block\n" if 8192 != read $fh, $block, 8192;
	my ($lsn_hi, $lsn_lo, $checksum) = unpack('VVv', $block);

	$lsn_hi = sprintf('%08X', $lsn_hi);
	$lsn_lo = sprintf('%08X', $lsn_lo);

	return ($lsn_hi, $lsn_lo, $checksum);
}

# Set umask so test directories and files are created with default permissions
umask(0077);

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init('-k');
$primary->adjust_conf('postgresql.conf', 'max_wal_size', '100MB');
$primary->start;

# Sanity checks for command line options.
$primary->command_fails(
	[ 'pg_waldump', '--save-fullpage' ],
	'--save-fullpage fails without path');
$primary->command_fails(
	[ 'pg_waldump', '--save-fullpage-fixup' ],
	'--save-fullpage-fixup fails without path');

# generate data/wal to examine
$primary->safe_psql('postgres', q(CREATE DATABASE db1));
$primary->safe_psql('db1',      <<EOF);
CREATE TABLE test_table AS SELECT generate_series(1,100) a;
CHECKPOINT;
UPDATE test_table SET a = a + 1;
CHECKPOINT;
EOF

# get the relation node, etc for the new table
my $relation = $primary->safe_psql('db1',
	q{SELECT CASE WHEN reltablespace = 0 THEN dattablespace ELSE reltablespace END || '/' || pg_database.oid || '/' || relfilenode FROM pg_class, pg_database WHERE relname = 'test_table' AND datname = current_database()}
);

diag $relation;

$primary->stop;
my $waldir = $primary->basedir . '/pgdata/pg_wal';
system("rsync -av " . $primary->basedir . " /tmp");
diag "using waldir: $waldir";
diag "waldir content: @{[qx/ls -l $waldir/]}";
#my $walfile = [glob("$waldir/*")]->[0];
my $tmp_folder = PostgreSQL::Test::Utils::tempdir;
#diag "using walfile: $walfile";

# # extract files
system(
	"pg_waldump --save-fullpage $tmp_folder/raw --relation $relation $waldir/000* || true"
);
system(
	"pg_waldump --save-fullpage-fixup $tmp_folder/fixup --relation $relation $waldir/000* || true"
);
system("rsync -av " . $tmp_folder . " /tmp/pgtesttmp/");

my $file_re =
  qr/^([0-9A-F]{8})-([0-9A-F]{8})[.][0-9]+[.][0-9]+[.][0-9]+[.][0-9]+(?:_vm|_init|_fsm)?$/;

my %checksums;
my %files;

# verify filename formats matches w/--save-fullpage
for my $fullpath (glob "$tmp_folder/raw/*")
{
	my $file = File::Basename::basename($fullpath);

	like($file, $file_re, "verify filename format for file $file");

	# save filename for later verification
	$files{$file}++;

	my ($hi_lsn_fn, $lo_lsn_fn) = ($file =~ $file_re);
	my ($hi_lsn_bk, $lo_lsn_bk, $checksum) = get_block_info($fullpath);

	# since no fixup, verify the lsn in the block comes before the file's lsn
	ok( $hi_lsn_fn . $lo_lsn_fn gt $hi_lsn_bk . $lo_lsn_bk,
		'verify file-based LSN precedes block-based one');

	# stash checksum for later comparisons
	$checksums{$file} = $checksum;
}

# verify filename formats matches w/--save-fullpage-fixup
for my $fullpath (glob "$tmp_folder/fixup/*")
{
	my $file = File::Basename::basename($fullpath);

	like($file, $file_re, "verify filename format for file $file");

	# save filename for later verification
	$files{$file}++;

	my ($hi_lsn_fn, $lo_lsn_fn) = ($file =~ $file_re);
	my ($hi_lsn_bk, $lo_lsn_bk, $checksum) = get_block_info($fullpath);

	# since fixup, verify the lsn in the block equals file lsn
	ok( $hi_lsn_fn . $lo_lsn_fn eq $hi_lsn_bk . $lo_lsn_bk,
		'verify file-based LSN is the same as block-based one');

	# verify checksum change; XXX: there could be valid clashes here,
	# just validate that the page matches the expected checksum instead?
	ok( $checksum == 0 || $checksums{$file} != $checksum,
		'check for checksum change or no checksum');
}

# validate that we ended up with some files output and they were the same
ok(keys %files > 0, 'verify we processed some files');
ok((grep { $_ != 2 } values %files) == 0,
	'ensure raw and fixup had same number of files');

done_testing();
