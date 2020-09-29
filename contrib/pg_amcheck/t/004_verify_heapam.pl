use strict;
use warnings;

use PostgresNode;
use TestLib;

use Test::More tests => 39;

# This regression test demonstrates that the verify_heapam() function supplied
# with the amcheck contrib module and depended upon by this pg_amcheck contrib
# module correctly identifies specific kinds of corruption within pages.  To
# test this, we need a mechanism to create corrupt pages with predictable,
# repeatable corruption.  The postgres backend cannot be expected to help us
# with this, as its design is not consistent with the goal of intentionally
# corrupting pages.
#
# Instead, we create a table to corrupt, and with careful consideration of how
# postgresql lays out heap pages, we seek to offsets within the page and
# overwrite deliberately chosen bytes with specific values calculated to
# corrupt the page in expected ways.  We then verify that verify_heapam
# reports the corruption, and that it runs without crashing.  Note that the
# backend cannot simply be started to run queries against the corrupt table, as
# the backend will crash, at least for some of the corruption types we
# generate.
#
# Autovacuum potentially touching the table in the background makes the exact
# behavior of this test harder to reason about.  We turn it off to keep things
# simpler.  We use a "belt and suspenders" approach, turning it off for the
# system generally in postgresql.conf, and turning it off specifically for the
# test table.
#
# This test depends on the table being written to the heap file exactly as we
# expect it to be, so we take care to arrange the columns of the table, and
# insert rows of the table, that give predictable sizes and locations within
# the table page.
#
# The HeapTupleHeaderData has 23 bytes of fixed size fields before the variable
# length t_bits[] array.  We have exactly 3 columns in the table, so natts = 3,
# t_bits is 1 byte long, and t_hoff = MAXALIGN(23 + 1) = 24.
#
# We're not too fussy about which datatypes we use for the test, but we do care
# about some specific properties.  We'd like to test both fixed size and
# varlena types.  We'd like some varlena data inline and some toasted.  And
# we'd like the layout of the table such that the datums land at predictable
# offsets within the tuple.  We choose a structure without padding on all
# supported architectures:
#
# 	a BIGINT
#	b TEXT
#	c TEXT
#
# We always insert a 7-ascii character string into field 'b', which with a
# 1-byte varlena header gives an 8 byte inline value.  We always insert a long
# text string in field 'c', long enough to force toast storage.
#
#
# We choose to read and write binary copies of our table's tuples, using perl's
# pack() and unpack() functions.  Perl uses a packing code system in which:
#
#	L = "Unsigned 32-bit Long",
#	S = "Unsigned 16-bit Short",
#	C = "Unsigned 8-bit Octet",
#	c = "signed 8-bit octet",
#	q = "signed 64-bit quadword"
#
# Each tuple in our table has a layout as follows:
#
#    xx xx xx xx            t_xmin: xxxx		offset = 0		L
#    xx xx xx xx            t_xmax: xxxx		offset = 4		L
#    xx xx xx xx          t_field3: xxxx		offset = 8		L
#    xx xx                   bi_hi: xx			offset = 12		S
#    xx xx                   bi_lo: xx			offset = 14		S
#    xx xx                ip_posid: xx			offset = 16		S
#    xx xx             t_infomask2: xx			offset = 18		S
#    xx xx              t_infomask: xx			offset = 20		S
#    xx                     t_hoff: x			offset = 22		C
#    xx                     t_bits: x			offset = 23		C
#    xx xx xx xx xx xx xx xx   'a': xxxxxxxx	offset = 24		q
#    xx xx xx xx xx xx xx xx   'b': xxxxxxxx	offset = 32		Cccccccc
#    xx xx xx xx xx xx xx xx   'c': xxxxxxxx	offset = 40		SSSS
#    xx xx xx xx xx xx xx xx      : xxxxxxxx	 ...continued	SSSS
#    xx xx                        : xx      	 ...continued	S
#
# We could choose to read and write columns 'b' and 'c' in other ways, but
# it is convenient enough to do it this way.  We define packing code
# constants here, where they can be compared easily against the layout.

use constant HEAPTUPLE_PACK_CODE => 'LLLSSSSSCCqCcccccccSSSSSSSSS';
use constant HEAPTUPLE_PACK_LENGTH => 58;     # Total size

# Read a tuple of our table from a heap page.
#
# Takes an open filehandle to the heap file, and the offset of the tuple.
#
# Rather than returning the binary data from the file, unpacks the data into a
# perl hash with named fields.  These fields exactly match the ones understood
# by write_tuple(), below.  Returns a reference to this hash.
#
sub read_tuple ($$)
{
	my ($fh, $offset) = @_;
	my ($buffer, %tup);
	seek($fh, $offset, 0);
	sysread($fh, $buffer, HEAPTUPLE_PACK_LENGTH);

	@_ = unpack(HEAPTUPLE_PACK_CODE, $buffer);
	%tup = (t_xmin => shift,
			t_xmax => shift,
			t_field3 => shift,
			bi_hi => shift,
			bi_lo => shift,
			ip_posid => shift,
			t_infomask2 => shift,
			t_infomask => shift,
			t_hoff => shift,
			t_bits => shift,
			a => shift,
			b_header => shift,
			b_body1 => shift,
			b_body2 => shift,
			b_body3 => shift,
			b_body4 => shift,
			b_body5 => shift,
			b_body6 => shift,
			b_body7 => shift,
			c1 => shift,
			c2 => shift,
			c3 => shift,
			c4 => shift,
			c5 => shift,
			c6 => shift,
			c7 => shift,
			c8 => shift,
			c9 => shift);
	# Stitch together the text for column 'b'
	$tup{b} = join('', map { chr($tup{"b_body$_"}) } (1..7));
	return \%tup;
}

# Write a tuple of our table to a heap page.
#
# Takes an open filehandle to the heap file, the offset of the tuple, and a
# reference to a hash with the tuple values, as returned by read_tuple().
# Writes the tuple fields from the hash into the heap file.
#
# The purpose of this function is to write a tuple back to disk with some
# subset of fields modified.  The function does no error checking.  Use
# cautiously.
#
sub write_tuple($$$)
{
	my ($fh, $offset, $tup) = @_;
	my $buffer = pack(HEAPTUPLE_PACK_CODE,
					$tup->{t_xmin},
					$tup->{t_xmax},
					$tup->{t_field3},
					$tup->{bi_hi},
					$tup->{bi_lo},
					$tup->{ip_posid},
					$tup->{t_infomask2},
					$tup->{t_infomask},
					$tup->{t_hoff},
					$tup->{t_bits},
					$tup->{a},
					$tup->{b_header},
					$tup->{b_body1},
					$tup->{b_body2},
					$tup->{b_body3},
					$tup->{b_body4},
					$tup->{b_body5},
					$tup->{b_body6},
					$tup->{b_body7},
					$tup->{c1},
					$tup->{c2},
					$tup->{c3},
					$tup->{c4},
					$tup->{c5},
					$tup->{c6},
					$tup->{c7},
					$tup->{c8},
					$tup->{c9});
	seek($fh, $offset, 0);
	syswrite($fh, $buffer, HEAPTUPLE_PACK_LENGTH);
	return;
}

# Set umask so test directories and files are created with default permissions
umask(0077);

# Set up the node.  Once we create and corrupt the table,
# autovacuum workers visiting the table could crash the backend.
# Disable autovacuum so that won't happen.
my $node = get_new_node('test');
$node->init;
$node->append_conf('postgresql.conf', 'autovacuum=off');

# Start the node and load the extensions.  We depend on both
# amcheck and pageinspect for this test.
$node->start;
my $port = $node->port;
my $pgdata = $node->data_dir;
$node->safe_psql('postgres', "CREATE EXTENSION amcheck");
$node->safe_psql('postgres', "CREATE EXTENSION pageinspect");

# Create the test table with precisely the schema that our
# corruption function expects.
$node->safe_psql(
	'postgres', qq(
		CREATE TABLE public.test (a BIGINT, b TEXT, c TEXT);
		ALTER TABLE public.test SET (autovacuum_enabled=false);
		ALTER TABLE public.test ALTER COLUMN c SET STORAGE EXTERNAL;
		CREATE INDEX test_idx ON public.test(a, b);
	));

my $rel = $node->safe_psql('postgres', qq(SELECT pg_relation_filepath('public.test')));
my $relpath = "$pgdata/$rel";

use constant ROWCOUNT => 14;
$node->safe_psql('postgres', qq(
	INSERT INTO public.test (a, b, c)
		VALUES (
			12345678,
			'abcdefg',
			repeat('w', 10000)
		);
	VACUUM FREEZE public.test
	)) for (1..ROWCOUNT);

my $relfrozenxid = $node->safe_psql('postgres',
	q(select relfrozenxid from pg_class where relname = 'test'));

# Find where each of the tuples is located on the page.
my @lp_off;
for my $tup (0..ROWCOUNT-1)
{
	push (@lp_off, $node->safe_psql('postgres', qq(
select lp_off from heap_page_items(get_raw_page('test', 'main', 0))
	offset $tup limit 1)));
}

# Check that pg_amcheck runs against the uncorrupted table without error.
$node->command_ok(['pg_amcheck', '-p', $port, 'postgres'],
				  'pg_amcheck test table, prior to corruption');

# Check that pg_amcheck runs against the uncorrupted table and index without error.
$node->command_ok(['pg_amcheck', '-p', $port, 'postgres'],
				  'pg_amcheck test table and index, prior to corruption');

$node->stop;

# Some #define constants from access/htup_details.h for use while corrupting.
use constant HEAP_HASNULL            => 0x0001;
use constant HEAP_XMAX_LOCK_ONLY     => 0x0080;
use constant HEAP_XMIN_COMMITTED     => 0x0100;
use constant HEAP_XMIN_INVALID       => 0x0200;
use constant HEAP_XMAX_COMMITTED     => 0x0400;
use constant HEAP_XMAX_INVALID       => 0x0800;
use constant HEAP_NATTS_MASK         => 0x07FF;
use constant HEAP_XMAX_IS_MULTI      => 0x1000;
use constant HEAP_KEYS_UPDATED       => 0x2000;

# Corrupt the tuples, one type of corruption per tuple.  Some types of
# corruption cause verify_heapam to skip to the next tuple without
# performing any remaining checks, so we can't exercise the system properly if
# we focus all our corruption on a single tuple.
#
my $file;
open($file, '+<', $relpath);
binmode $file;

for (my $tupidx = 0; $tupidx < ROWCOUNT; $tupidx++)
{
	my $offset = $lp_off[$tupidx];
	my $tup = read_tuple($file, $offset);

	# Sanity-check that the data appears on the page where we expect.
	if ($tup->{a} ne '12345678' || $tup->{b} ne 'abcdefg')
	{
		fail('Page layout differs from our expectations');
		$node->clean_node;
		exit;
	}

	if ($tupidx == 0)
	{
		# Corruptly set xmin < relfrozenxid
		$tup->{t_xmin} = 3;
		$tup->{t_infomask} &= ~HEAP_XMIN_COMMITTED;
		$tup->{t_infomask} &= ~HEAP_XMIN_INVALID;
	}
	elsif ($tupidx == 1)
	{
		# Corruptly set xmin < relfrozenxid, further back
		$tup->{t_xmin} = 4026531839;		# Note circularity of xid comparison
		$tup->{t_infomask} &= ~HEAP_XMIN_COMMITTED;
		$tup->{t_infomask} &= ~HEAP_XMIN_INVALID;
	}
	elsif ($tupidx == 2)
	{
		# Corruptly set xmax < relminmxid;
		$tup->{t_xmax} = 4026531839;		# Note circularity of xid comparison
		$tup->{t_infomask} &= ~HEAP_XMAX_INVALID;
	}
	elsif ($tupidx == 3)
	{
		# Corrupt the tuple t_hoff, but keep it aligned properly
		$tup->{t_hoff} += 128;
	}
	elsif ($tupidx == 4)
	{
		# Corrupt the tuple t_hoff, wrong alignment
		$tup->{t_hoff} += 3;
	}
	elsif ($tupidx == 5)
	{
		# Corrupt the tuple t_hoff, underflow but correct alignment
		$tup->{t_hoff} -= 8;
	}
	elsif ($tupidx == 6)
	{
		# Corrupt the tuple t_hoff, underflow and wrong alignment
		$tup->{t_hoff} -= 3;
	}
	elsif ($tupidx == 7)
	{
		# Corrupt the tuple to look like it has lots of attributes, not just 3
		$tup->{t_infomask2} |= HEAP_NATTS_MASK;
	}
	elsif ($tupidx == 8)
	{
		# Corrupt the tuple to look like it has lots of attributes, some of
		# them null.  This falsely creates the impression that the t_bits
		# array is longer than just one byte, but t_hoff still says otherwise.
		$tup->{t_infomask} |= HEAP_HASNULL;
		$tup->{t_infomask2} |= HEAP_NATTS_MASK;
		$tup->{t_bits} = 0xAA;
	}
	elsif ($tupidx == 9)
	{
		# Same as above, but this time t_hoff plays along
		$tup->{t_infomask} |= HEAP_HASNULL;
		$tup->{t_infomask2} |= (HEAP_NATTS_MASK & 0x40);
		$tup->{t_bits} = 0xAA;
		$tup->{t_hoff} = 32;
	}
	elsif ($tupidx == 10)
	{
		# Corrupt the bits in column 'b' 1-byte varlena header
		$tup->{b_header} = 0x80;
	}
	elsif ($tupidx == 11)
	{
		# Corrupt the bits in column 'c' toast pointer
		$tup->{c6} = 41;
		$tup->{c7} = 41;
	}
	elsif ($tupidx == 12)
	{
		# Set both HEAP_XMAX_LOCK_ONLY and HEAP_KEYS_UPDATED
		$tup->{t_infomask} |= HEAP_XMAX_LOCK_ONLY;
		$tup->{t_infomask2} |= HEAP_KEYS_UPDATED;
	}
	elsif ($tupidx == 13)
	{
		# Set both HEAP_XMAX_COMMITTED and HEAP_XMAX_IS_MULTI
		$tup->{t_infomask} |= HEAP_XMAX_COMMITTED;
		$tup->{t_infomask} |= HEAP_XMAX_IS_MULTI;
	}
	write_tuple($file, $offset, $tup);
}
close($file);

# Run verify_heapam on the corrupted file
$node->start;

my $result = $node->safe_psql(
			'postgres',
			q(SELECT * FROM verify_heapam('test', check_toast := true)));
is ($result,
"0|1||inserting transaction ID is from before freeze cutoff: 3 vs. $relfrozenxid
0|2||inserting transaction ID is from before freeze cutoff: 4026531839 vs. $relfrozenxid
0|3||updating transaction ID is from before relation cutoff: 4026531839 vs. $relfrozenxid
0|4||data begins at offset beyond the tuple length: 152 vs. 58
0|4||data offset differs from expected: 152 vs. 24 (3 attributes, no nulls)
0|5||data offset differs from expected: 27 vs. 24 (3 attributes, no nulls)
0|6||data offset differs from expected: 16 vs. 24 (3 attributes, no nulls)
0|7||data offset differs from expected: 21 vs. 24 (3 attributes, no nulls)
0|8||number of attributes exceeds maximum expected for table: 2047 vs. 3
0|9||data offset differs from expected: 24 vs. 280 (2047 attributes, has nulls)
0|10||number of attributes exceeds maximum expected for table: 67 vs. 3
0|11|1|attribute ends at offset beyond total tuple length: 416848000 vs. 58 (attribute length 4294967295)
0|12|2|final toast chunk number differs from expected value: 0 vs. 6
0|12|2|toasted value missing from toast table
0|13||updating transaction ID marked incompatibly as keys updated and locked only
0|14||multitransaction ID is from before relation cutoff: 0 vs. 1",
"Expected verify_heapam output");

# Each table corruption message is returned with a standard header, and we can
# check for those headers to verify that corruption is being reported.  We can
# also check for each individual corruption that we would expect to see.
my @corruption_re = (

	# standard header
	qr/relname=test,blkno=\d*,offnum=\d*,attnum=\d*/,

	# individual detected corruptions
	qr/attribute ends at offset beyond total tuple length: \d+ vs. \d+ \(attribute length \d+\)/,
	qr/data begins at offset beyond the tuple length: \d+ vs. \d+/,
	qr/data offset differs from expected: \d+ vs. \d+ \(\d+ attributes, has nulls\)/,
	qr/data offset differs from expected: \d+ vs. \d+ \(\d+ attributes, no nulls\)/,
	qr/final toast chunk number differs from expected value: \d+ vs. \d+/,
	qr/inserting transaction ID is from before freeze cutoff: \d+ vs. \d+/,
	qr/multitransaction ID is from before relation cutoff: \d+ vs. \d+/,
	qr/number of attributes exceeds maximum expected for table: \d+ vs. \d+/,
	qr/toasted value missing from toast table/,
	qr/updating transaction ID is from before relation cutoff: \d+ vs. \d+/,
	qr/updating transaction ID marked incompatibly as keys updated and locked only/,
);

$node->command_like(
	['pg_amcheck', '--check-toast', '--skip-indexes', '-p', $port, 'postgres'], $_,
	"pg_amcheck reports: $_"
	) for(@corruption_re);

$node->teardown_node;
$node->clean_node;
