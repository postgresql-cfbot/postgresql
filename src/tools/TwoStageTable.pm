#----------------------------------------------------------------------
#
# TwoStageTable.pm
#    Perl module for generating compact C tables and lookup functions
#    optimized for sparse numeric key distributions.
#
# The module creates a two-stage table (Offset and Index) for numeric values.
# This approach is well suited for data clustered into ranges with variable-
# sized gaps between them — for example, Unicode code points and character
# encodings.
#
#
# How it works.
#
# Essentially, we split the key space into fixed-size ranges of N values
# (where N = 1 << SHIFT).
#
# The module creates two tables:
# 1. Offset
#     Contains offsets for the table Index.
#     Stores the beginning of the range for a number.
#
# 2. Index
#     Stores fixed-size ranges (pages) one after another. Each cell contains
#     the value returned by the user callback for the corresponding key —
#     typically an index into a separate user-provided data table, but it
#     can be any numeric value.
#
# Algorithm for obtaining a value for a given key (using SHIFT = 8 and the
# example tables shown below):
#     1. We have the key 0x42 (Unicode code point for 'B', i.e. 66).
#     2. Calculate its index in the Offset table: 0x42 >> SHIFT
#        (8 bit = 256) = 0. offset[0] = 256.
#     3. After obtaining the offset of the range start in the Index table, we
#        calculate the specific position among 256 values for the key 0x42:
#        RANGE_MASK = (1 << SHIFT) - 1
#        offset[0] + (cp & RANGE_MASK) = 256 + 66 = 322
#        index[322] = 76 (the value pushed for 'B' is 0x42 + 10 = 76).
#
# The first range of the Index table (positions 0..RANGE_SIZE-1) is reserved
# as a dummy range filled with the dummy value (0 by default; configurable
# via the second argument to new()). Any Offset table entry that has not
# been explicitly assigned holds the value 0 and therefore points to this
# dummy range, making the lookup return the dummy value (acting like a
# NULL result). The same dummy value is also returned by the lookup
# function for keys whose top-level offset index is out of range.
#
# For example:
#     use TwoStageTable;
#
#     my %data;
#     my $tst = new TwoStageTable(8);
#
#     foreach my $id (0x41..0x5A, 0x61..0x7A) {
#         $tst->push($id);
#         $data{$id} = $id + 10;
#     }
#
#     my ($offset, $index, $func) = $tst->generate(
#         'latin_greek_table',
#         'get_index',
#         sub { $data{$_[0]} }
#     );
#
#     print join("\n", $offset, $index, $func);
#
# Result:
#
# static const uint16 latin_greek_table_offset[2] =
# {
#     256, 0
# };
#
# static const uint16 latin_greek_table_index[379] =
# {
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 75, 76, 77,
#     78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
#     97, 98, 99, 100, 0, 0, 0, 0, 0, 0, 107, 108, 109, 110, 111, 112, 113, 114,
#     115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
#     130, 131, 132
# };
#
# static uint16
# get_index(char32_t cp)
# {
#     uint16 offset_idx, offset;
#
#     offset_idx = cp >> 8;
#
#     if (offset_idx > 1)
#         return 0;
#
#     offset = latin_greek_table_offset[offset_idx];
#
#     return latin_greek_table_index[offset + (cp & 255)];
# }
#
# We can balance the two tables by changing the SHIFT value, which controls
# the trade-off between their sizes:
#   - smaller SHIFT  → smaller pages, fewer empty slots within each page
#                      (denser Index table), but more pages and therefore a
#                      larger Offset table.
#   - larger SHIFT   → bigger pages with more empty slots inside (sparser
#                      Index table), but fewer pages and therefore a smaller
#                      Offset table.
#
# For example, let's modify the example above:
#     my $tst = new TwoStageTable(4); # Not 8, but 4.
#
# The result for the same numbers:
#
# static const uint16 latin_greek_table_offset[9] =
# {
#     0, 0, 0, 0, 16, 32, 48, 64, 0
# };
#
# static const uint16 latin_greek_table_index[75] =
# {
#     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 75, 76, 77, 78, 79, 80,
#     81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99,
#     100, 0, 0, 0, 0, 0, 0, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116,
#     117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131,
#     132
# };
#
# static uint16
# get_index(char32_t cp)
# {
#     uint16 offset_idx, offset;
#
#     offset_idx = cp >> 4;
#
#     if (offset_idx > 8)
#         return 0;
#
#     offset = latin_greek_table_offset[offset_idx];
#
#     return latin_greek_table_index[offset + (cp & 15)];
# }
#
# We can see that the tables have become more "balanced".
#
#
# Disadvantages.
#
# The algorithm performs well when values are clustered into ranges with
# moderate gaps between them (typical for Unicode property data and
# character encodings). For example, values spread across ranges
# 100..500, 1000..1200, 10000..30000, and so on — everything will be fine
# in the algorithm.
#
# For example, in the worst case scenario, let's take two ranges of numbers:
# 10..150 and 5000000..10000000. Tables with very large dummy data will be
# constructed for these values. It seems that these problems can be solved,
# but this does not apply to the current tasks of using this algorithm.
#
# Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/tools/TwoStageTable.pm
#
#----------------------------------------------------------------------

package TwoStageTable;

use strict;
use warnings FATAL => 'all';
use Text::Wrap qw(wrap);

# new($range_shift, $dummy)
#    Constructor for the sparse table generator object.
#
#    Initializes paging parameters and internal state used to build the
#    two-level table (offset + index). If $range_shift is not provided,
#    it defaults to 8, which gives a range size of 2^8 = 256 entries.
#
#    $dummy is the value used for empty Index-table slots and as the
#    return value of the generated lookup function for keys outside the
#    populated range. Defaults to 0.
#
#    Internal fields:
#      keys              - array of all registered numeric keys
#      max               - maximum key seen so far
#      offset_table_size - size of the offset table (filled at generate())
#      range_shift       - bit count, controls range granularity
#      range_size        - number of positions in one range (1 << range_shift)
#      range_mask        - bitmask for extracting low bits ($range_size - 1)
#      dummy             - fill value for empty Index slots / out-of-range lookups
sub new
{
	my ($class, $range_shift, $dummy) = @_;
	my ($range_size, $range_mask);

	$range_shift ||= 8;
	$range_size = 1 << $range_shift;
	$range_mask = $range_size - 1;
	$dummy //= 0;

	die "the dummy value must be an integer"
	  unless $dummy =~ /^-?[0-9]+$/;

	return bless {
		keys => [],
		max => 0,
		index_uint => "uint16",
		offset_uint => "uint16",
		offset_table_size => 0,
		range_shift => $range_shift,
		range_size => $range_size,
		range_mask => $range_mask,
		dummy => $dummy
	}, $class;
}

# push($key)
#    Append a new numeric key in the generator state.
#
#    The key must be a non-negative integer; otherwise the function dies
#    with an error message.
sub push
{
	my ($tst, $key) = @_;

	die "the key must be a number" unless $key =~ /^[0-9]+$/;

	CORE::push @{ $tst->{keys} }, $key;
}

# generate($table_name, $func_name, $callback)
#    Main generation routine that produces C tables and lookup function
#    from all previously added keys.
#
#    return [Offset table, Index table, lookup function].
sub generate
{
	my ($tst, $table_name, $func_name, $callback) = @_;
	my (@sorted, @offsets, @data, $pos, $table_size);
	my ($uint_offset);

	die "no values for table generation and functions"
	  unless scalar(@{ $tst->{keys} });

	# It is not essential, but for consistent table output, it is better
	# to sort the data.
	@sorted = sort { $a <=> $b } @{ $tst->{keys} };
	$table_size = (($sorted[-1] >> $tst->{range_shift}) + 1);

	# We immediately allocate the required size for the table.
	$offsets[$table_size] = 0;
	$pos = $tst->{range_size};

	foreach my $key (@sorted)
	{
		my $offset_index = $key >> $tst->{range_shift};
		my $offset = $offsets[$offset_index];

		unless (defined $offset)
		{
			$offset = $pos;
			$offsets[$offset_index] = $offset;
			$pos += $tst->{range_size};

			$uint_offset = _uint_type($offset);
		}

		my $index = $key & $tst->{range_mask};
		my $value = $callback->($key);

		$data[ $offset + $index ] = $value;
	}

	# Pick the Index-table cell type from the actual data range, including
	# $tst->{dummy} so the type is wide enough to hold it (e.g. dummy =
	# 0xFFFF over uint8 data forces uint16). If any value is negative, fall
	# back to a signed type that fits both extremes.
	my @range = sort { $a <=> $b } grep { defined $_ } @data, $tst->{dummy};
	my ($min, $max) = ($range[0], $range[-1]);

	$tst->{offset_table_size} = $table_size;
	$tst->{offset_uint} = $uint_offset;
	$tst->{index_uint} = $min < 0 ? _int_type($min, $max) : _uint_type($max);

	return (
		$tst->_table_offset(\@offsets, $table_name),
		$tst->_table_index(\@data, $table_name),
		$tst->function($func_name, $table_name));
}

sub function
{
	my ($tst, $func_name, $table_name) = @_;

	my $offset_name = "$table_name\_offset";
	my $index_name = "$table_name\_index";

	return <<FUNCTION;
/*
 * Lookup for tables:
 *     $offset_name and $index_name.
 */
static $tst->{index_uint}
$func_name(char32_t cp)
{
	$tst->{offset_uint}		offset_idx,
				offset;

	offset_idx = cp >> $tst->{range_shift};

	if (offset_idx > $tst->{offset_table_size})
		return $tst->{dummy};

	offset = $offset_name\[offset_idx];

	return $index_name\[offset + (cp & $tst->{range_mask})];
}
FUNCTION
}

sub _table_offset
{
	my ($tst, $table, $name) = @_;

	# Offset entries are structural pointers into the Index table; an
	# unassigned entry must stay 0 so it points at the first (dummy) range
	# regardless of the user's $tst->{dummy} value.
	my $table_text =
	  $tst->table($table, "$name\_offset", $tst->{offset_uint}, 0);

	my $comment = <<COMMENT;
/*
 * The table contains offsets to table $name\_index.
 */
COMMENT
	return "$comment$table_text";
}

sub _table_index
{
	my ($tst, $table, $name) = @_;

	return $tst->table($table, "$name\_index", $tst->{index_uint},
		$tst->{dummy});
}

# table($table, $name, $uint_type, $fill)
#    Formats raw table data as pretty-printed C static array declaration.
#    $fill is the value used to replace undefined slots in $table.
sub table
{
	my ($tst, $table, $name, $uint_type, $fill) = @_;

	my @values = map { defined $_ ? $_ : $fill } @$table;
	my $length = scalar @values;

	# Wrap the comma-separated values to ~80 columns with pgindent's indent.
	local $Text::Wrap::columns = 80;
	local $Text::Wrap::huge = 'overflow';

	my $body = wrap("\t", "\t", join(", ", @values));

	return join "\n",
	  "static const $uint_type $name\[$length] =\n{",
	  $body,
	  "};";
}

# _uint_type($number)
#    Internal function that determines the size of uint for a number.
sub _uint_type
{
	my $num = 0 + $_[0];

	# uint8: (1 << 8) - 1
	if ($num <= (((1 << 7) - 1) | (1 << 7)))
	{
		return "uint8";
	}

	# uint16: (1 << 16) - 1
	if ($num <= (((1 << 15) - 1) | (1 << 15)))
	{
		return "uint16";
	}

	# uint32: (1 << 32) - 1
	if ($num <= (((1 << 31) - 1) | (1 << 31)))
	{
		return "uint32";
	}

	# uint64: (1 << 64) - 1
	if ($num <= (((1 << 63) - 1) | (1 << 63)))
	{
		return "uint64";
	}

	die "value is greater than uint64: $num";
}

# _int_type($min, $max)
#    Internal function that picks the smallest signed C type from
#    {int8, int16, int32, int64} that fits both $min and $max.
sub _int_type
{
	my ($min, $max) = @_;

	# int8
	if ($min >= -(1 << 7) && $max <= ((1 << 7) - 1))
	{
		return "int8";
	}

	# int16
	if ($min >= -(1 << 15) && $max <= ((1 << 15) - 1))
	{
		return "int16";
	}

	# int32
	if ($min >= -(1 << 31) && $max <= ((1 << 31) - 1))
	{
		return "int32";
	}

	# int64
	if ($min >= -(1 << 63) && $max <= ((1 << 63) - 1))
	{
		return "int64";
	}

	die "value is out of int64 range: min=$min max=$max";
}

1;
