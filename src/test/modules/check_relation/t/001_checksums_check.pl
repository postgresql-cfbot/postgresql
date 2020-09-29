use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 59;

our $CHECKSUM_UINT16_OFFSET = 4;
our $PD_UPPER_UINT16_OFFSET = 7;
our $BLOCKSIZE;
our $TOTAL_NB_ERR = 0;

sub get_block
{
	my ($filename, $blkno) = @_;
	my $block;

	open(my $infile, '<', $filename) or die;
	binmode($infile);

	my $success = read($infile, $block, $BLOCKSIZE, ($blkno * $BLOCKSIZE));
	die($!) if not defined $success;

	close($infile);

	return($block);
}

sub overwrite_block
{
	my ($filename, $block, $blkno) = @_;

	open(my $outfile, '>', $filename) or die;
	binmode ($outfile);

	my $nb = syswrite($outfile, $block, $BLOCKSIZE, ($blkno * $BLOCKSIZE));

	die($!) if not defined $nb;
	die("Write error") if ($nb != $BLOCKSIZE);

	$outfile->flush();

	close($outfile);
}

sub get_uint16_from_page
{
	my ($block, $offset) = @_;

	return (unpack("S*", $block))[$offset];
}

sub set_uint16_to_page
{
	my ($block, $data, $offset) = @_;

	my $pack = pack("S", $data);

	# vec with 16B or more won't preserve endianness
	vec($block, 2*$offset, 8) = (unpack('C*', $pack))[0];
	vec($block, (2*$offset) + 1, 8) = (unpack('C*', $pack))[1];

	return $block;
}

sub check_checksums_call
{
	my ($node, $relname) = @_;

	my ($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT COUNT(*)"
		. " FROM pg_catalog.pg_check_relation('$relname')"
	);

	 return ($stderr eq '');
}

sub check_checksums_nb_error
{
	my ($node, $nb, $pattern) = @_;

	my ($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT COUNT(*)"
        . " FROM (SELECT pg_catalog.pg_check_relation(oid, 'main')"
        . "   FROM pg_class WHERE relkind in ('r', 'i', 'm')) AS s"
	);

	is($cmdret, 0, 'Function should run successfully');
	like($stderr, $pattern, 'Error output should match expectations');
	is($stdout, $nb, "Should have $nb error");

	$TOTAL_NB_ERR += $nb;
}

sub check_pg_stat_database_nb_error
{
	my ($node) = @_;

	my ($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT "
		. " sum(checksum_failures)"
		. " FROM pg_catalog.pg_stat_database"
	);

	is($cmdret, 0, 'Function should run successfully');
	is($stderr, '', 'Function should run successfully');
	is($stdout, $TOTAL_NB_ERR, "Should have $TOTAL_NB_ERR error");
}

sub get_checksums_errors
{
	my ($node, $nb, $pattern) = @_;

	my ($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT"
		. " relid::regclass::text, forknum, failed_blocknum,"
		. " expected_checksum, found_checksum"
		. " FROM (SELECT (pg_catalog.pg_check_relation(oid)).*"
        . "   FROM pg_class WHERE relkind in ('r','i', 'm')) AS s"
	);

	 is($cmdret, '0', 'Function should run successfully');
	 like($stderr, $pattern, 'Error output should match expectations');

	 $TOTAL_NB_ERR += $nb;

	 return $stdout;
}

# This function will perform various test by modifying the specified block at
# the specified uint16 offset, checking that the corruption is correctly
# detected, and finally restore the specified block to its original content.
sub corrupt_and_test_block
{
	my ($node, $filename, $blkno, $offset, $fake_data) = @_;

	check_checksums_nb_error($node, 0, qr/^$/);

	check_pg_stat_database_nb_error($node);

	$node->stop();

	my $original_block = get_block($filename, 0);
	my $original_data = get_uint16_from_page($original_block, $offset);

	isnt($original_data, $fake_data,
		"The fake data at offset $offset should be different"
		. " from the existing one");

	my $new_block = set_uint16_to_page($original_block, $fake_data, $offset);
	isnt($original_data, get_uint16_from_page($new_block, $offset),
		"The fake data at offset $offset should have been changed in memory");

	overwrite_block($filename, $new_block, 0);

	my $written_data = get_uint16_from_page(get_block($filename, 0), $offset);
	isnt($original_data, $written_data,
		"The data written at offset $offset should be different"
		. " from the original one");
	is(get_uint16_from_page($new_block, $offset), $written_data,
		"The data written at offset $offset should be the same"
		. " as the one in memory");
	is($written_data, $fake_data,
		"The data written at offset $offset should be the one"
		. "	we wanted to write");

	$node->start();

	check_checksums_nb_error($node, 1, qr/invalid page in block $blkno/);

	my $expected_checksum;
	my $found_checksum = get_uint16_from_page($new_block,
		$CHECKSUM_UINT16_OFFSET);
	if ($offset == $PD_UPPER_UINT16_OFFSET)
	{
		# A checksum can't be computed if it's detected as PageIsNew(), so the
		# function returns NULL for the computed checksum
		$expected_checksum = '';
	}
	else
	{
		$expected_checksum = get_uint16_from_page($original_block,
			$CHECKSUM_UINT16_OFFSET);
	}

	my $det = get_checksums_errors($node, 1, qr/invalid page in block $blkno/);
	is($det, "t1|0|0|$expected_checksum|$found_checksum",
		"The checksums error for modification at offset $offset"
		. " should be detected");

	$node->stop();

	$new_block = set_uint16_to_page($original_block, $original_data, $offset);
	is($original_data, get_uint16_from_page($new_block, $offset),
		"The data at offset $offset should have been restored in memory");

	overwrite_block($filename, $new_block, 0);
	is($original_data, get_uint16_from_page(get_block($filename, $blkno),
			$offset),
		"The data at offset $offset should have been restored on disk");

	$node->start();

	check_checksums_nb_error($node, 0, qr/^$/);
}

if (exists $ENV{MY_PG_REGRESS})
{
	$ENV{PG_REGRESS} = $ENV{MY_PG_REGRESS};
}

my $node = get_new_node('main');

my %params;
$params{'extra'} = ['--data-checksums'];
$node->init(%params);

$node->start();

$ENV{PGOPTIONS} = '--client-min-messages=WARNING';

my ($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT"
	. " current_setting('data_checksums')");

is($stdout, 'on', 'Data checksums should be enabled');

($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT"
	. " current_setting('block_size')");

$BLOCKSIZE = $stdout;

$node->safe_psql(
	'postgres', q|
	CREATE TABLE public.t1(id integer);
	CREATE INDEX t1_id_idx ON public.t1 (id);
	INSERT INTO public.t1 SELECT generate_series(1, 100);
	CREATE VIEW public.v1 AS SELECT * FROM t1;
	CREATE MATERIALIZED VIEW public.mv1 AS SELECT * FROM t1;
	CREATE SEQUENCE public.s1;
	CREATE UNLOGGED TABLE public.u_t1(id integer);
	CREATE INDEX u_t1_id_idx ON public.u_t1 (id);
	INSERT INTO public.u_t1 SELECT generate_series(1, 100);
	CHECKPOINT;
|);

# Check sane behavior on various objects type, including those that don't have
# a storage.
is(check_checksums_call($node, 't1'), '1', 'Can check a table');
is(check_checksums_call($node, 't1_id_idx'), '1', 'Can check an index');
is(check_checksums_call($node, 'v1'), '', 'Cannot check a view');
is(check_checksums_call($node, 'mv1'), '1', 'Can check a materialized view');
is(check_checksums_call($node, 's1'), '1', 'Can check a sequence');
is(check_checksums_call($node, 'u_t1'), '1', 'Can check an unlogged table');
is(check_checksums_call($node, 'u_t1_id_idx'), '1', 'Can check an unlogged index');

# get the underlying heap absolute path
($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT"
	. " current_setting('data_directory') || '/' || pg_relation_filepath('t1')"
);

isnt($stdout, '', 'A relfilenode should be returned');

my $filename = $stdout;

check_checksums_nb_error($node, 0, qr/^$/);

check_pg_stat_database_nb_error($node);

my $fake_uint16 = hex '0x0000';

# Test with a modified checksum.  We use a zero checksum here as it's the only
# one that cannot exist on a checksummed page.  We also don't have an easy way
# to compute what the checksum would be after a modification in a random place
# in the block.
corrupt_and_test_block($node, $filename, 0, $CHECKSUM_UINT16_OFFSET,
	$fake_uint16);

# Test corruption making the block looks like it's PageIsNew().
corrupt_and_test_block($node, $filename, 0, $PD_UPPER_UINT16_OFFSET,
	$fake_uint16);
