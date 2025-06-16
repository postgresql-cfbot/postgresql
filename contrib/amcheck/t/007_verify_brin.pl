# Copyright (c) 2021-2025, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my $node;
my $blksize;
my $meta_page_blkno = 0;

#
# Test set-up
#
$node = PostgreSQL::Test::Cluster->new('test');
$node->init(no_data_checksums => 1);
$node->append_conf('postgresql.conf', 'autovacuum=off');
$node->start;
$blksize = int($node->safe_psql('postgres', 'SHOW block_size;'));
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));

# Tests

# Test flow:
# - create all necessary relations and indexes for all test cases
# - stop the node
# - insert corruptions for all test cases
# - start the node
# - assertions
#
# This way we avoid waiting for the node to restart for each test, which speeds up the tests.

my @tests = (
    {
        # invalid meta page type

        find     => pack('S', 0xF091),
        replace  => pack('S', 0xAAAA),
        blkno    => $meta_page_blkno,
        expected => wrap('metapage is corrupted')
    },
    {
        # invalid meta page magic word

        find     => pack('L', 0xA8109CFA),
        replace  => pack('L', 0xBB109CFB),
        blkno    => $meta_page_blkno,
        expected => wrap('metapage is corrupted'),
    },
    {
        # invalid meta page index version

        find     => pack('L*', 0xA8109CFA, 1),
        replace  => pack('L*', 0xA8109CFA, 2),
        blkno    => $meta_page_blkno,
        expected => wrap('metapage is corrupted')
    },
    {
        # pages_per_range below lower limit

        find     => pack('L*', 0xA8109CFA, 1, 128),
        replace  => pack('L*', 0xA8109CFA, 1, 0),
        blkno    => $meta_page_blkno,
        expected => wrap('metapage is corrupted')
    },
    {
        # pages_per_range above upper limit

        find     => pack('L*', 0xA8109CFA, 1, 128),
        replace  => pack('L*', 0xA8109CFA, 1, 131073),
        blkno    => $meta_page_blkno,
        expected => wrap('metapage is corrupted')
    },
    {
        # last_revmap_page below lower limit

        find     => pack('L*', 0xA8109CFA, 1, 128, 1),
        replace  => pack('L*', 0xA8109CFA, 1, 128, 0),
        blkno    => $meta_page_blkno,
        expected => wrap('metapage is corrupted'),
    },
    {

        # last_revmap_page beyond index relation size

        find     => pack('L*', 0xA8109CFA, 1, 128, 1),
        replace  => pack('L*', 0xA8109CFA, 1, 128, 100),
        blkno    => $meta_page_blkno,
        expected => wrap('metapage is corrupted'),
    },
    {
        # invalid revmap page type

        find     => pack('S', 0xF092),
        replace  => pack('S', 0xAAAA),
        blkno    => 1, # revmap page
        expected => wrap('revmap page is expected at block 1, last revmap page 1'),
    },
    {
        # revmap item points beyond index relation size
        # replace (2,1) with (100,1)

        find     => pack('S*', 0, 2, 1),
        replace  => pack('S*', 0, 100, 1),
        blkno    => 1, # revmap page
        expected => wrap('revmap item points to a non existing block 100, '
            . 'index max block 2. Range blkno: 0, revmap item: (1,0)')
    },
    {
        # invalid regular page type

        find     => pack('S', 0xF093),
        replace  => pack('S', 0xAAAA),
        blkno    => 2, # regular page
        expected => wrap('revmap item points to the page which is not regular (blkno: 2). '
            . 'Range blkno: 0, revmap item: (1,0)')
    },
    {
        # revmap item points beyond regular page max offset
        # replace (2,1) with (2,2)

        find     => pack('S*', 0, 2, 1),
        replace  => pack('S*', 0, 2, 2),
        blkno    => 1, # revmap page
        expected => wrap('revmap item offset number 2 is greater than regular page 2 max offset 1. '
            . 'Range blkno: 0, revmap item: (1,0)')
    },
    {
        # invalid index tuple range blkno

        find     => pack('LCC', 0, 0xA8, 0x01),
        replace  => pack('LCC', 1, 0xA8, 0x01),
        blkno    => 2, # regular page
        expected => wrap('index tuple has invalid blkno 1. Range blkno: 0, revmap item: (1,0), index tuple: (2,1)')
    },
    {
        # range beyond the table size and is not empty

        find     => pack('LCC', 0, 0xA8, 0x01),
        replace  => pack('LCC', 0, 0x88, 0x01),
        blkno    => 2, # regular page
        expected => wrap('the range is beyond the table size, but is not marked as empty, table size: 0 blocks. '
            . 'Range blkno: 0, revmap item: (1,0), index tuple: (2,1)')
    },
    {
        # corrupt index tuple data offset
        # here  0x00, 0x00, 0x00 is padding and '.' is varlena len byte

        find       => pack('LCCCC', 0, 0x08, 0x00, 0x00, 0x00) . '(.)' . 'aaaaa',
        replace    => pack('LCCCC', 0, 0x1F, 0x00, 0x00, 0x00) . '$1' . 'aaaaa',
        blkno      => 2, # regular page
        table_data => sub {
            my ($test_struct) = @_;
            return qq(INSERT INTO $test_struct->{table_name} (a) VALUES ('aaaaa'););
        },
        expected   => qr/index tuple header length 31 is greater than tuple len ..\. \QRange blkno: 0, revmap item: (1,0), index tuple: (2,1)\E/
    },
    {
        # empty range index tuple doesn't have null bitmap

        find     => pack('LCC', 0, 0xA8, 0x01),
        replace  => pack('LCC', 0, 0x28, 0x01),
        blkno    => 2, # regular page
        expected => wrap('empty range index tuple doesn\'t have null bitmap. '
            . 'Range blkno: 0, revmap item: (1,0), index tuple: (2,1)')
    },
    {
        # empty range index tuple all_nulls -> false

        find     => pack('LCC', 0, 0xA8, 0x01),
        replace  => pack('LCC', 0, 0xA8, 0x00),
        blkno    => 2, # regular page
        expected => wrap('empty range index tuple attribute 0 with allnulls is false. '
            . 'Range blkno: 0, revmap item: (1,0), index tuple: (2,1)')
    },
    {
        # empty range index tuple has_nulls -> true

        find     => pack('LCC', 0, 0xA8, 0x01),
        replace  => pack('LCC', 0, 0xA8, 0x03),
        blkno    => 2, # regular page
        expected => wrap('empty range index tuple attribute 0 with hasnulls is true. '
            . 'Range blkno: 0, revmap item: (1,0), index tuple: (2,1)')
    },
    {
        # invalid index tuple data
        # replace varlena len with FF - should work with any endianness

        find       => pack('LCCCC', 0, 0x08, 0x00, 0x00, 0x00) . '.' . 'aaaaa',
        replace    => pack('LCCCCC', 0, 0x08, 0x00, 0x00, 0x00, 0xFF) . 'aaaaa',
        blkno      => 2, # regular page
        table_data => sub {
            my ($test_struct) = @_;
            return qq(INSERT INTO $test_struct->{table_name} (a) VALUES ('aaaaa'););
        },
        expected   => qr/attribute 0 stored value 0 with length -1 ends at offset 127 beyond total tuple length ..\.\Q Range blkno: 0, revmap item: (1,0), index tuple: (2,1)\E/
    },
    {
        # orphan index tuple
        # replace valid revmap item with (0,0)

        find       => pack('S*', 0, 2, 1),
        replace    => pack('S*', 0, 0, 0),
        blkno      => 1, # revmap page
        table_data => sub {
            my ($test_struct) = @_;
            return qq(INSERT INTO $test_struct->{table_name} (a) VALUES ('aaaaa'););
        },
        expected   => wrap("revmap doesn't point to index tuple. Range blkno: 0, revmap item: (1,0), index tuple: (2,1)")
    },
    {
        # range is marked as empty_range, but heap has some data for the range

        find     => pack('LCC', 0, 0x88, 0x03),
        replace  => pack('LCC', 0, 0xA8, 0x01),
        blkno      => 2, # regular page
        table_data => sub {
            my ($test_struct) = @_;
            return qq(INSERT INTO $test_struct->{table_name} (a) VALUES (null););
        },
        expected   => wrap('range is marked as empty but contains qualified live tuples. Range blkno: 0, heap tid (0,1)')
    },
    {
        # range hasnulls & allnulls are false, but heap contains null values for the range

        find     => pack('LCC', 0, 0x88, 0x02),
        replace  => pack('LCC', 0, 0x88, 0x00),
        blkno      => 2, # regular page
        table_data => sub {
            my ($test_struct) = @_;
            return qq(INSERT INTO $test_struct->{table_name} (a) VALUES (null), ('aaaaa'););
        },
        expected   => wrap('range hasnulls and allnulls are false, but contains a null value. Range blkno: 0, heap tid (0,1)')
    },
    {
        # range allnulls is true, but heap contains non-null values for the range

        find     => pack('LCC', 0, 0x88, 0x02),
        replace  => pack('LCC', 0, 0x88, 0x01),
        blkno      => 2, # regular page
        table_data => sub {
            my ($test_struct) = @_;
            return qq(INSERT INTO $test_struct->{table_name} (a) VALUES (null), ('aaaaa'););
        },
        expected   => wrap('range allnulls is true, but contains nonnull value. Range blkno: 0, heap tid (0,2)')
    },
    {
        # consistent function return FALSE for the valid heap value
        # replace "ccccc" with "bbbbb" so that min_max index was too narrow

        find       => 'ccccc',
        replace    => 'bbbbb',
        blkno      => 2, # regular page
        table_data => sub {
            my ($test_struct) = @_;
            return qq(INSERT INTO $test_struct->{table_name} (a) VALUES ('aaaaa'), ('ccccc'););
        },
        expected   => wrap('heap tuple inconsistent with index. Range blkno: 0, heap tid (0,2)')
    }
);


# init test data
my $i = 1;
foreach my $test_struct (@tests) {

    $test_struct->{table_name} = 't' . $i++;
    $test_struct->{index_name} = $test_struct->{table_name} . '_brin_idx';

    my $test_data_sql = '';
    if (exists $test_struct->{table_data}) {
        $test_data_sql = $test_struct->{table_data}->($test_struct);
    }

    $node->safe_psql('postgres', qq(
        CREATE TABLE $test_struct->{table_name} (a TEXT);
        $test_data_sql
        CREATE INDEX $test_struct->{index_name} ON $test_struct->{table_name} USING BRIN (a);
    ));

    $test_struct->{relpath} = relation_filepath($test_struct->{index_name});
}

# corrupt index
$node->stop;

foreach my $test_struct (@tests) {
    string_replace_block(
        $test_struct->{relpath},
        $test_struct->{find},
        $test_struct->{replace},
        $test_struct->{blkno}
    );
}

# assertions
$node->start;

foreach my $test_struct (@tests) {
    my ($result, $stdout, $stderr) = $node->psql('postgres', qq(SELECT brin_index_check('$test_struct->{index_name}', true, true)));
    like($stderr, $test_struct->{expected});
}


# Helpers

# Returns the filesystem path for the named relation.
sub relation_filepath {
    my ($relname) = @_;

    my $pgdata = $node->data_dir;
    my $rel = $node->safe_psql('postgres',
        qq(SELECT pg_relation_filepath('$relname')));
    die "path not found for relation $relname" unless defined $rel;
    return "$pgdata/$rel";
}

sub string_replace_block {
    my ($filename, $find, $replace, $blkno) = @_;

    my $fh;
    open($fh, '+<', $filename) or BAIL_OUT("open failed: $!");
    binmode $fh;

    my $offset = $blkno * $blksize;
    my $buffer;

    sysseek($fh, $offset, 0) or BAIL_OUT("seek failed: $!");
    sysread($fh, $buffer, $blksize) or BAIL_OUT("read failed: $!");

    $buffer =~ s/$find/'"' . $replace . '"'/gee;

    sysseek($fh, $offset, 0) or BAIL_OUT("seek failed: $!");
    syswrite($fh, $buffer) or BAIL_OUT("write failed: $!");

    close($fh) or BAIL_OUT("close failed: $!");

    return;
}

sub wrap
{
    my $input = @_;
    return qr/\Q$input\E/
}

done_testing();