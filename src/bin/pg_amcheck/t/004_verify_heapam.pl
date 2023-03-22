
# Copyright (c) 2021-2023, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

# This regression test demonstrates that the pg_amcheck binary correctly
# identifies specific kinds of corruption within pages.  To test this, we need
# a mechanism to create corrupt pages with predictable, repeatable corruption.
# The postgres backend cannot be expected to help us with this, as its design
# is not consistent with the goal of intentionally corrupting pages.
#
# Instead, we create a table to corrupt, and with careful consideration of how
# postgresql lays out heap pages, we seek to offsets within the page and
# overwrite deliberately chosen bytes with specific values calculated to
# corrupt the page in expected ways.  We then verify that pg_amcheck reports
# the corruption, and that it runs without crashing.  Note that the backend
# cannot simply be started to run queries against the corrupt table, as the
# backend will crash, at least for some of the corruption types we generate.
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
# We choose to read and write binary copies of our table's tuples, using perl's
# pack() and unpack() functions.  Perl uses a packing code system in which:
#
#	l = "signed 32-bit Long",
#	L = "Unsigned 32-bit Long",
#	S = "Unsigned 16-bit Short",
#	C = "Unsigned 8-bit Octet",
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
#    xx xx xx xx xx xx xx xx   'a': xxxxxxxx	offset = 24		LL
#    xx xx xx xx xx xx xx xx   'b': xxxxxxxx	offset = 32		CCCCCCCC
#    xx xx xx xx xx xx xx xx   'c': xxxxxxxx	offset = 40		CCllLL
#    xx xx xx xx xx xx xx xx      : xxxxxxxx	 ...continued
#    xx xx                        : xx      	 ...continued
#
# We could choose to read and write columns 'b' and 'c' in other ways, but
# it is convenient enough to do it this way.  We define packing code
# constants here, where they can be compared easily against the layout.

use constant HEAPTUPLE_PACK_CODE => 'LLLSSSSSCCLLCCCCCCCCCCllLL';
use constant HEAPTUPLE_PACK_LENGTH => 58;    # Total size

# Read a tuple of our table from a heap page.
#
# Takes an open filehandle to the heap file, and the offset of the tuple.
#
# Rather than returning the binary data from the file, unpacks the data into a
# perl hash with named fields.  These fields exactly match the ones understood
# by write_tuple(), below.  Returns a reference to this hash.
#
sub read_tuple
{
	my ($fh, $offset) = @_;
	my ($buffer, %tup);
	sysseek($fh, $offset, 0)
	  or BAIL_OUT("sysseek failed: $!");
	defined(sysread($fh, $buffer, HEAPTUPLE_PACK_LENGTH))
	  or BAIL_OUT("sysread failed: $!");

	@_ = unpack(HEAPTUPLE_PACK_CODE, $buffer);
	%tup = (
		t_xmin          => shift,
		t_xmax          => shift,
		t_field3        => shift,
		bi_hi           => shift,
		bi_lo           => shift,
		ip_posid        => shift,
		t_infomask2     => shift,
		t_infomask      => shift,
		t_hoff          => shift,
		t_bits          => shift,
		a_1             => shift,
		a_2             => shift,
		b_header        => shift,
		b_body1         => shift,
		b_body2         => shift,
		b_body3         => shift,
		b_body4         => shift,
		b_body5         => shift,
		b_body6         => shift,
		b_body7         => shift,
		c_va_header     => shift,
		c_va_vartag     => shift,
		c_va_rawsize    => shift,
		c_va_extinfo    => shift,
		c_va_valueid    => shift,
		c_va_toastrelid => shift);
	# Stitch together the text for column 'b'
	$tup{b} = join('', map { chr($tup{"b_body$_"}) } (1 .. 7));
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
sub write_tuple
{
	my ($fh, $offset, $tup) = @_;
	my $buffer = pack(
		HEAPTUPLE_PACK_CODE,
		$tup->{t_xmin},       $tup->{t_xmax},
		$tup->{t_field3},     $tup->{bi_hi},
		$tup->{bi_lo},        $tup->{ip_posid},
		$tup->{t_infomask2},  $tup->{t_infomask},
		$tup->{t_hoff},       $tup->{t_bits},
		$tup->{a_1},          $tup->{a_2},
		$tup->{b_header},     $tup->{b_body1},
		$tup->{b_body2},      $tup->{b_body3},
		$tup->{b_body4},      $tup->{b_body5},
		$tup->{b_body6},      $tup->{b_body7},
		$tup->{c_va_header},  $tup->{c_va_vartag},
		$tup->{c_va_rawsize}, $tup->{c_va_extinfo},
		$tup->{c_va_valueid}, $tup->{c_va_toastrelid});
	sysseek($fh, $offset, 0)
	  or BAIL_OUT("sysseek failed: $!");
	defined(syswrite($fh, $buffer, HEAPTUPLE_PACK_LENGTH))
	  or BAIL_OUT("syswrite failed: $!");
	return;
}

# Set umask so test directories and files are created with default permissions
umask(0077);

my $pred_xmax;
my $pred_posid;
my $aborted_xid;
# Set up the node.  Once we create and corrupt the table,
# autovacuum workers visiting the table could crash the backend.
# Disable autovacuum so that won't happen.
my $node = PostgreSQL::Test::Cluster->new('test');
$node->init;
$node->append_conf('postgresql.conf', 'autovacuum=off');
$node->append_conf('postgresql.conf', 'max_prepared_transactions=10');

# Start the node and load the extensions.  We depend on both
# amcheck and pageinspect for this test.
$node->start;
my $port   = $node->port;
my $pgdata = $node->data_dir;
$node->safe_psql('postgres', "CREATE EXTENSION amcheck");
$node->safe_psql('postgres', "CREATE EXTENSION pageinspect");

# Get a non-zero datfrozenxid
$node->safe_psql('postgres', qq(VACUUM FREEZE));

# Create the test table with precisely the schema that our corruption function
# expects.
$node->safe_psql(
	'postgres', qq(
		CREATE TABLE public.test (a BIGINT, b TEXT, c TEXT);
		ALTER TABLE public.test SET (autovacuum_enabled=false);
		ALTER TABLE public.test ALTER COLUMN c SET STORAGE EXTERNAL;
		CREATE INDEX test_idx ON public.test(a, b);
	));

# We want (0 < datfrozenxid < test.relfrozenxid).  To achieve this, we freeze
# an otherwise unused table, public.junk, prior to inserting data and freezing
# public.test
$node->safe_psql(
	'postgres', qq(
		CREATE TABLE public.junk AS SELECT 'junk'::TEXT AS junk_column;
		ALTER TABLE public.junk SET (autovacuum_enabled=false);
		VACUUM FREEZE public.junk
	));

my $rel = $node->safe_psql('postgres',
	qq(SELECT pg_relation_filepath('public.test')));
my $relpath = "$pgdata/$rel";

# Insert data and freeze public.test
my $ROWCOUNT = 43; # Total row count in this page.
my $ROWCOUNT_HOT = 27; # Row count related to test of HOT chains validations and redirected LP.
my $ROWCOUNT_NONHOT = $ROWCOUNT-$ROWCOUNT_HOT;

# First insert data needed for non-HOT chain validation.
$node->safe_psql(
	'postgres', qq(
	INSERT INTO public.test (a, b, c)
		SELECT
			x'DEADF9F9DEADF9F9'::bigint,
			'abcdefg',
			repeat('w', 10000)
	FROM generate_series(1, $ROWCOUNT_NONHOT);
	VACUUM FREEZE public.test;)
);

# Data for Redirected LP.
$node->safe_psql(
	'postgres', qq(
		INSERT INTO public.test (a, b, c)
			VALUES ( x'DEADF9F9DEADF9F9'::bigint, 'abcdefg', generate_series(1,2));
		UPDATE public.test SET c = 'a' WHERE c = '1';
		UPDATE public.test SET c = 'a' WHERE c = '2';
		INSERT INTO public.test (a, b, c)
			VALUES ( x'DEADF9F9DEADF9F9'::bigint, 'abcdefg', generate_series(3,6));
		UPDATE public.test SET c = 'a' WHERE c = '3';
		UPDATE public.test SET c = 'a' WHERE c = '4';
	));

# Negative test case of HOT-pruning with aborted tuple.
$node->safe_psql(
        'postgres', qq(
                BEGIN;
                        UPDATE public.test SET c = 'a' WHERE c = '5';
                ABORT;
		VACUUM FREEZE public.test;
        ));

# Next update on any tuple will be stored at the same place of tuple inserted by aborted transaction.
# This should not raise any corruption.
$node->safe_psql(
        'postgres', qq(
                        UPDATE public.test SET c = 'a' WHERE c = '6';
                VACUUM FREEZE public.test;
        ));

# Data for HOT chains validation, so not calling VACUUM FREEZE.
$node->safe_psql(
	'postgres', qq(
		INSERT INTO public.test (a, b, c)
			VALUES ( x'DEADF9F9DEADF9F9'::bigint, 'abcdefg', generate_series(7,15));
		UPDATE public.test SET c = 'a' WHERE c = '7';
		UPDATE public.test SET c = 'a' WHERE c = '10';
		UPDATE public.test SET c = 'a' WHERE c = '11';
		UPDATE public.test SET c = 'a' WHERE c = '12';
		UPDATE public.test SET c = 'a' WHERE c = '13';
		UPDATE public.test SET c = 'a' WHERE c = '14';
		UPDATE public.test SET c = 'a' WHERE c = '15';
	));

# Need one aborted transaction to test corruption in HOT chains.
$node->safe_psql(
	'postgres', qq(
		BEGIN;
			UPDATE public.test SET c = 'a' WHERE c = '9';
		ABORT;
	));

# Need one in-progress transaction to test few corruption in HOT chains.
# We are creating PREPARE TRANSACTION here as these will not be aborted
# even if we stop the node.
$node->safe_psql(
	'postgres', qq(
		BEGIN;
			PREPARE TRANSACTION 'in_progress_tx';
	));
my $in_progress_xid = $node->safe_psql(
				'postgres', qq(
					SELECT transaction FROM pg_prepared_xacts;
				));

my $relfrozenxid = $node->safe_psql('postgres',
	q(select relfrozenxid from pg_class where relname = 'test'));
my $datfrozenxid = $node->safe_psql('postgres',
	q(select datfrozenxid from pg_database where datname = 'postgres'));

# Sanity check that our 'test' table has a relfrozenxid newer than the
# datfrozenxid for the database, and that the datfrozenxid is greater than the
# first normal xid.  We rely on these invariants in some of our tests.
if ($datfrozenxid <= 3 || $datfrozenxid >= $relfrozenxid)
{
	$node->clean_node;
	plan skip_all =>
	  "Xid thresholds not as expected: got datfrozenxid = $datfrozenxid, relfrozenxid = $relfrozenxid";
	exit;
}

# Find where each of the tuples is located on the page.
my @lp_off = split '\n', $node->safe_psql(
	'postgres', qq(
	    SELECT CASE WHEN lp_flags = 2 THEN -1 ELSE lp_off END
	    FROM heap_page_items(get_raw_page('test', 'main', 0))
		WHERE lp <= $ROWCOUNT
    )
);
is(scalar @lp_off, $ROWCOUNT, "acquired row offsets");

# Sanity check that our 'test' table on disk layout matches expectations.  If
# this is not so, we will have to skip the test until somebody updates the test
# to work on this platform.
$node->stop;
my $file;
open($file, '+<', $relpath)
  or BAIL_OUT("open failed: $!");
binmode $file;

my $ENDIANNESS;
for (my $tupidx = 0; $tupidx < $ROWCOUNT; $tupidx++)
{
	my $offnum = $tupidx + 1;        # offnum is 1-based, not zero-based
	my $offset = $lp_off[$tupidx];
	if ($offset == -1)
	{
		next;
	}
	my $tup = read_tuple($file, $offset);

	# Sanity-check that the data appears on the page where we expect.
	my $a_1 = $tup->{a_1};
	my $a_2 = $tup->{a_2};
	my $b   = $tup->{b};
	if ($a_1 != 0xDEADF9F9 || $a_2 != 0xDEADF9F9 || $b ne 'abcdefg')
	{
		close($file);    # ignore errors on close; we're exiting anyway
		$node->clean_node;
		plan skip_all =>
		  sprintf(
			"Page layout of index %d differs from our expectations: expected (%x, %x, \"%s\"), got (%x, %x, \"%s\")", $tupidx,
			0xDEADF9F9, 0xDEADF9F9, "abcdefg", $a_1, $a_2, $b);
		exit;
	}

	# Determine endianness of current platform from the 1-byte varlena header
	$ENDIANNESS = $tup->{b_header} == 0x11 ? "little" : "big";
}
close($file)
  or BAIL_OUT("close failed: $!");
$node->start;

# Ok, Xids and page layout look ok.  We can run corruption tests.

# Check that pg_amcheck runs against the uncorrupted table without error.
$node->command_ok(
	[ 'pg_amcheck', '-p', $port, 'postgres' ],
	'pg_amcheck test table, prior to corruption');

# Check that pg_amcheck runs against the uncorrupted table and index without error.
$node->command_ok([ 'pg_amcheck', '-p', $port, 'postgres' ],
	'pg_amcheck test table and index, prior to corruption');

$node->stop;

# Some #define constants from access/htup_details.h for use while corrupting.
use constant HEAP_HASNULL        => 0x0001;
use constant HEAP_XMAX_LOCK_ONLY => 0x0080;
use constant HEAP_XMIN_COMMITTED => 0x0100;
use constant HEAP_XMIN_INVALID   => 0x0200;
use constant HEAP_XMAX_COMMITTED => 0x0400;
use constant HEAP_XMAX_INVALID   => 0x0800;
use constant HEAP_NATTS_MASK     => 0x07FF;
use constant HEAP_XMAX_IS_MULTI  => 0x1000;
use constant HEAP_KEYS_UPDATED   => 0x2000;
use constant HEAP_HOT_UPDATED    => 0x4000;
use constant HEAP_ONLY_TUPLE     => 0x8000;
use constant HEAP_UPDATED        => 0x2000;

# Helper function to generate a regular expression matching the header we
# expect verify_heapam() to return given which fields we expect to be non-null.
sub header
{
	my ($blkno, $offnum, $attnum) = @_;
	return
	  qr/heap table "postgres\.public\.test", block $blkno, offset $offnum, attribute $attnum:\s+/ms
	  if (defined $attnum);
	return
	  qr/heap table "postgres\.public\.test", block $blkno, offset $offnum:\s+/ms
	  if (defined $offnum);
	return qr/heap table "postgres\.public\.test", block $blkno:\s+/ms
	  if (defined $blkno);
	return qr/heap table "postgres\.public\.test":\s+/ms;
}

# Corrupt the tuples, one type of corruption per tuple.  Some types of
# corruption cause verify_heapam to skip to the next tuple without
# performing any remaining checks, so we can't exercise the system properly if
# we focus all our corruption on a single tuple.
#
my @expected;
open($file, '+<', $relpath)
  or BAIL_OUT("open failed: $!");
binmode $file;

for (my $tupidx = 0; $tupidx < $ROWCOUNT; $tupidx++)
{
	my $offnum = $tupidx + 1;        # offnum is 1-based, not zero-based
	my $offset = $lp_off[$tupidx];
	my $header = header(0, $offnum, undef);
	# offset -1 means its redirected lp.
	if ($offset == -1)
	{	# at offnum 19 we will unset HEAP_ONLY_TUPLE and HEAP_UPDATED flags.
		if ($offnum == 17)
		{
			push @expected,
			  qr/${header}redirected line pointer points to a non-heap-only tuple at offset \d+/;
			push @expected,
			  qr/${header}redirected line pointer points to a non-heap-updated tuple at offset \d+/;
		}
		elsif ($offnum == 18)
		{
			# we re-set lp offset to 17, we need to rewrite the 4 bytes values so that line pointer will be
			# lp.off = 17, lp_flags = 2, lp_len = 0.
			if ($ENDIANNESS eq 'little')
			{
				sysseek($file, 92, 0)
				  or BAIL_OUT("sysseek failed: $!");
				syswrite(
					$file,
					pack("L",
						0x00010011)
				) or BAIL_OUT("syswrite failed: $!");
			}
			else
			{
				sysseek($file, 92, 0)
				  or BAIL_OUT("sysseek failed: $!");
				syswrite(
					$file,
					pack("L",
						0x11000100)
				) or BAIL_OUT("syswrite failed: $!");

			}
			push @expected,
			  qr/${header}redirected line pointer points to another redirected line pointer at offset \d+/;
		}
		elsif ($offnum == 22)
		{
			# we re-set lp offset to 25, we need to rewrite the 4 bytes values so that line pointer will be
			# lp.off = 25, lp_flags = 2, lp_len = 0.
			if ($ENDIANNESS eq 'little')
			{
				sysseek($file, 108, 0)
				  or BAIL_OUT("sysseek failed: $!");
				syswrite(
					$file,
					pack("L",
						0x00010019)
				) or BAIL_OUT("syswrite failed: $!");
			}
			else
			{
				sysseek($file, 108, 0)
				  or BAIL_OUT("sysseek failed: $!");
				syswrite(
					$file,
					pack("L",
						0x19000100)
				) or BAIL_OUT("syswrite failed: $!");

			}
			push @expected,
			  qr/${header}redirect line pointer points to offset \d+, but offset \d+ also points there/;
		}
		next;
	}
	my $tup = read_tuple($file, $offset);

	if ($offnum == 1)
	{
		# Corruptly set xmin < relfrozenxid
		my $xmin = $relfrozenxid - 1;
		$tup->{t_xmin} = $xmin;
		$tup->{t_infomask} &= ~HEAP_XMIN_COMMITTED;
		$tup->{t_infomask} &= ~HEAP_XMIN_INVALID;

		# Expected corruption report
		push @expected,
		  qr/${header}xmin $xmin precedes relation freeze threshold 0:\d+/;
	}
	if ($offnum == 2)
	{
		# Corruptly set xmin < datfrozenxid
		my $xmin = 3;
		$tup->{t_xmin} = $xmin;
		$tup->{t_infomask} &= ~HEAP_XMIN_COMMITTED;
		$tup->{t_infomask} &= ~HEAP_XMIN_INVALID;

		push @expected,
		  qr/${$header}xmin $xmin precedes oldest valid transaction ID 0:\d+/;
	}
	elsif ($offnum == 3)
	{
		# Corruptly set xmin < datfrozenxid, further back, noting circularity
		# of xid comparison.
		my $xmin = 4026531839;
		$tup->{t_xmin} = $xmin;
		$tup->{t_infomask} &= ~HEAP_XMIN_COMMITTED;
		$tup->{t_infomask} &= ~HEAP_XMIN_INVALID;

		push @expected,
		  qr/${$header}xmin ${xmin} precedes oldest valid transaction ID 0:\d+/;
	}
	elsif ($offnum == 4)
	{
		# Corruptly set xmax < relminmxid;
		my $xmax = 4026531839;
		$tup->{t_xmax} = $xmax;
		$tup->{t_infomask} &= ~HEAP_XMAX_INVALID;

		push @expected,
		  qr/${$header}xmax ${xmax} precedes oldest valid transaction ID 0:\d+/;
	}
	elsif ($offnum == 5)
	{
		# Corrupt the tuple t_hoff, but keep it aligned properly
		$tup->{t_hoff} += 128;

		push @expected,
		  qr/${$header}data begins at offset 152 beyond the tuple length 58/,
		  qr/${$header}tuple data should begin at byte 24, but actually begins at byte 152 \(3 attributes, no nulls\)/;
	}
	elsif ($offnum == 6)
	{
		# Corrupt the tuple t_hoff, wrong alignment
		$tup->{t_hoff} += 3;

		push @expected,
		  qr/${$header}tuple data should begin at byte 24, but actually begins at byte 27 \(3 attributes, no nulls\)/;
	}
	elsif ($offnum == 7)
	{
		# Corrupt the tuple t_hoff, underflow but correct alignment
		$tup->{t_hoff} -= 8;

		push @expected,
		  qr/${$header}tuple data should begin at byte 24, but actually begins at byte 16 \(3 attributes, no nulls\)/;
	}
	elsif ($offnum == 8)
	{
		# Corrupt the tuple t_hoff, underflow and wrong alignment
		$tup->{t_hoff} -= 3;

		push @expected,
		  qr/${$header}tuple data should begin at byte 24, but actually begins at byte 21 \(3 attributes, no nulls\)/;
	}
	elsif ($offnum == 9)
	{
		# Corrupt the tuple to look like it has lots of attributes, not just 3
		$tup->{t_infomask2} |= HEAP_NATTS_MASK;

		push @expected,
		  qr/${$header}number of attributes 2047 exceeds maximum expected for table 3/;
	}
	elsif ($offnum == 10)
	{
		# Corrupt the tuple to look like it has lots of attributes, some of
		# them null.  This falsely creates the impression that the t_bits
		# array is longer than just one byte, but t_hoff still says otherwise.
		$tup->{t_infomask}  |= HEAP_HASNULL;
		$tup->{t_infomask2} |= HEAP_NATTS_MASK;
		$tup->{t_bits} = 0xAA;

		push @expected,
		  qr/${$header}tuple data should begin at byte 280, but actually begins at byte 24 \(2047 attributes, has nulls\)/;
	}
	elsif ($offnum == 11)
	{
		# Same as above, but this time t_hoff plays along
		$tup->{t_infomask}  |= HEAP_HASNULL;
		$tup->{t_infomask2} |= (HEAP_NATTS_MASK & 0x40);
		$tup->{t_bits} = 0xAA;
		$tup->{t_hoff} = 32;

		push @expected,
		  qr/${$header}number of attributes 67 exceeds maximum expected for table 3/;
	}
	elsif ($offnum == 12)
	{
		# Overwrite column 'b' 1-byte varlena header and initial characters to
		# look like a long 4-byte varlena
		#
		# On little endian machines, bytes ending in two zero bits (xxxxxx00 bytes)
		# are 4-byte length word, aligned, uncompressed data (up to 1G).  We set the
		# high six bits to 111111 and the lower two bits to 00, then the next three
		# bytes with 0xFF using 0xFCFFFFFF.
		#
		# On big endian machines, bytes starting in two zero bits (00xxxxxx bytes)
		# are 4-byte length word, aligned, uncompressed data (up to 1G).  We set the
		# low six bits to 111111 and the high two bits to 00, then the next three
		# bytes with 0xFF using 0x3FFFFFFF.
		#
		$tup->{b_header} = $ENDIANNESS eq 'little' ? 0xFC : 0x3F;
		$tup->{b_body1}  = 0xFF;
		$tup->{b_body2}  = 0xFF;
		$tup->{b_body3}  = 0xFF;

		$header = header(0, $offnum, 1);
		push @expected,
		  qr/${header}attribute with length \d+ ends at offset \d+ beyond total tuple length \d+/;
	}
	elsif ($offnum == 13)
	{
		# Corrupt the bits in column 'c' toast pointer
		$tup->{c_va_valueid} = 0xFFFFFFFF;

		$header = header(0, $offnum, 2);
		push @expected, qr/${header}toast value \d+ not found in toast table/;
	}
	elsif ($offnum == 14)
	{
		# Set both HEAP_XMAX_COMMITTED and HEAP_XMAX_IS_MULTI
		$tup->{t_infomask} |= HEAP_XMAX_COMMITTED;
		$tup->{t_infomask} |= HEAP_XMAX_IS_MULTI;
		$tup->{t_xmax} = 4;

		push @expected,
		  qr/${header}multitransaction ID 4 equals or exceeds next valid multitransaction ID 1/;
	}
	elsif ($offnum == 15)
	{
		# Set both HEAP_XMAX_COMMITTED and HEAP_XMAX_IS_MULTI
		$tup->{t_infomask} |= HEAP_XMAX_COMMITTED;
		$tup->{t_infomask} |= HEAP_XMAX_IS_MULTI;
		$tup->{t_xmax} = 4000000000;

		push @expected,
		  qr/${header}multitransaction ID 4000000000 precedes relation minimum multitransaction ID threshold 1/;
	}
	elsif ($offnum == 16)    # Last offnum must equal ROWCOUNT
	{
		# Corruptly set xmin > next_xid to be in the future.
		my $xmin = 123456;
		$tup->{t_xmin} = $xmin;
		$tup->{t_infomask} &= ~HEAP_XMIN_COMMITTED;
		$tup->{t_infomask} &= ~HEAP_XMIN_INVALID;

		push @expected,
		  qr/${$header}xmin ${xmin} equals or exceeds next valid transaction ID 0:\d+/;
	}
	# Test for redirected line pointer.
	# offnum 17 and 18 are redirected line pointer, so don't need any tuple
	# validation.
	elsif ($offnum == 19)
	{
		# unset HEAP_ONLY_TUPLE and HEAP_UPDATED flag.
		$tup->{t_infomask2} &= ~HEAP_ONLY_TUPLE;
		$tup->{t_infomask} &= ~HEAP_UPDATED;
	}
	# offnum 18 is redirected lp and is redirected to offset 20,
	# We have corrupted it to route its lp.off to point it to line pointer at
	# offset 17.

	# Test related to HOT chains.
	elsif ($offnum == 28)
	{
		# Unset HEAP_HOT_UPDATED.
		$tup->{t_infomask2} &= ~HEAP_HOT_UPDATED;
		$pred_xmax = $tup->{t_xmax}; # to be used for tuple at offnum 29.
		$pred_posid = $tup->{ip_posid}; # to be used for tuple at offnum 29.
		push @expected,
		  qr/${header}non-heap-only update produced a heap-only tuple at offset \d+/;
	}
	elsif ($offnum == 29)
	{
		# Set ip_posid and t_xmax from ip_posid and t_xmax of tuple at offnum 28.
		$tup->{t_xmax} = $pred_xmax;
		$tup->{ip_posid} = $pred_posid;
		push @expected,
		  qr/${header}tuple points to new version at offset \d+, but offset \d+ also points there/;
	}
	elsif ($offnum == 30)
	{
		# Get aborted xid, that is needed to test corruption at offnum 31.
		$aborted_xid = $tup->{t_xmax};
	}
	elsif ($offnum == 31)
	{
		# Set xmin to aborted xid.
		$tup->{t_xmin} = $aborted_xid;
		$tup->{t_infomask} &= ~HEAP_XMIN_COMMITTED;
		push @expected,
		  qr/${header}tuple with aborted xmin \d+ was updated to produce a tuple at offset \d+ with committed xmin \d+/;
	}
	elsif ($offnum == 32)
	{
		# Raised corruption as root of HOT chain can't be HEAP_ONLY_TUPLE.
		# set HEAP_ONLY_TUPLE.
		$tup->{t_infomask2} |= HEAP_ONLY_TUPLE;
		push @expected,
		  qr/${header}tuple is root of chain but is marked as heap-only tuple/;
	}
	elsif ($offnum == 33)
	{
		# Next updated Tuple at offnum 37 is corrupted.
		push @expected,
		  qr/${header}heap-only update produced a non-heap only tuple at offset \d+/;
	}
	elsif ($offnum == 34)
	{
		# set xmax to invalid transaction id.
		$tup->{t_xmax} = 0;
		push @expected,
		  qr/${header}tuple has been HOT updated, but xmax is 0/;
	}
	elsif ($offnum == 35)
	{
		# set xmax to invalid transaction id.
		$tup->{t_xmin} = $in_progress_xid;
		$tup->{t_infomask} &= ~HEAP_XMIN_COMMITTED;
		push @expected,
		  qr/${header}tuple with in-progress xmin \d+ was updated to produce a tuple at offset \d+ with committed xmin \d+/;
	}
	elsif ($offnum == 36)
	{
		# set xmax to invalid transaction id.
		$tup->{t_xmin} = $aborted_xid;
		$tup->{t_xmax} = $in_progress_xid;
		$tup->{t_infomask} &= ~HEAP_XMIN_COMMITTED;
		push @expected,
		  qr/${header}tuple with aborted xmin \d+ was updated to produce a tuple at offset \d+ with in-progress xmin \d+/;
	}
	# Tuple at offnum 37 is an update of tuple at offnum 28.

	# Tuple at offnum 38 is an update of tuple at offnum 31.

	# Tuple at offnum 39 is an update of tuple at offnum 32.

	elsif($offnum == 40)
	{
		# Unset HEAP_ONLY_TUPLE, corrupton will be raised for tuple at offnum #33
		$tup->{t_infomask2} &= ~HEAP_ONLY_TUPLE;
	}
	# Tuple at offnum 41 is an update of corrupted tuple at offnum 34.
	# Tuple at offnum 42 is an update of corrupted tuple at offnum 35.
	# Tuple at offnum 43 is an update of tuple at offnum 36..
	elsif ($offnum == 43)
	{
		# set xmax to invalid transaction id.
		$tup->{t_xmin} = $in_progress_xid;
		$tup->{t_infomask} &= ~HEAP_XMIN_COMMITTED;
	}
	# Tuple at offnum 44 is an update of tuple at offnum 30.
	# offset 44 is an updated tuple of tuple at offset #30 and was updated by an aborted transaction.
	# this is needed to have aborted transaction xid to test corruption related to aborted transaction at offset #36.
	write_tuple($file, $offset, $tup);
}
close($file)
  or BAIL_OUT("close failed: $!");
$node->start;

# Run pg_amcheck against the corrupt table with epoch=0, comparing actual
# corruption messages against the expected messages
$node->command_checks_all(
	[ 'pg_amcheck', '--no-dependent-indexes', '-p', $port, 'postgres' ],
	2, [@expected], [], 'Expected corruption message output');
$node->safe_psql(
        'postgres', qq(
                        COMMIT PREPARED 'in_progress_tx';
        ));

$node->teardown_node;
$node->clean_node;

done_testing();
