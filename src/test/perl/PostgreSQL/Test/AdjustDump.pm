
# Copyright (c) 2024-2025, PostgreSQL Global Development Group

=pod

=head1 NAME

PostgreSQL::Test::AdjustDump - helper module for dump and restore tests

=head1 SYNOPSIS

  use PostgreSQL::Test::AdjustDump;

  # Adjust contents of dump output file so that dump output from original
  # regression database and that from the restored regression database match
  $dump = adjust_regress_dumpfile($dump, $original);

=head1 DESCRIPTION

C<PostgreSQL::Test::AdjustDump> encapsulates various hacks needed to
compare the results of dump and retore tests

=cut

package PostgreSQL::Test::AdjustDump;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';

our @EXPORT = qw(
  adjust_regress_dumpfile
);

=pod

=head1 ROUTINES

=over

=item $dump = adjust_regress_dumpfile($dump, $original)

If we take dump of the regression database left behind after running regression
tests, restore the dump, and take dump of the restored regression database, the
outputs of both the dumps differ. Some regression tests purposefully create
some child tables in such a way that their column orders differ from column
orders of their respective parents. In the restored database, however, their
column orders are same as that of their respective parents. Thus the column
orders of these child tables in the original database and those in the restored
database differ, causing difference in the dump outputs. See MergeAttributes()
and dumpTableSchema() for details.

This routine rearranges the column declarations in these C<CREATE TABLE ... INHERITS>
statements in the dump file from original database to match that from the
restored database.

Additionally it adjusts blank and new lines to avoid noise.

Arguments:

=over

=item C<dump>: Contents of dump file

=item C<original>: 1 indicates that the given dump file is from the original
database, else 0

=back

Returns the adjusted dump text.

=cut

sub adjust_regress_dumpfile
{
	my ($dump, $original) = @_;

	# use Unix newlines
	$dump =~ s/\r\n/\n/g;
	# Suppress blank lines, as some places in pg_dump emit more or fewer.
	$dump =~ s/\n\n+/\n/g;

	# Adjust the CREATE TABLE ... INHERITS statements.
	if ($original)
	{
		$dump =~ s/(^CREATE\sTABLE\sgenerated_stored_tests\.gtestxx_4\s\()
				   (\n\s+b\sinteger),
				   (\n\s+a\sinteger)/$1$3,$2/mgx;
		$dump =~ s/(^CREATE\sTABLE\spublic\.test_type_diff2_c1\s\()
				   (\n\s+int_four\sbigint),
				   (\n\s+int_eight\sbigint),
				   (\n\s+int_two\ssmallint)/$1$4,$2,$3/mgx;
		$dump =~ s/(CREATE\sTABLE\spublic\.test_type_diff2_c2\s\()
				   (\n\s+int_eight\sbigint),
				   (\n\s+int_two\ssmallint),
				   (\n\s+int_four\sbigint)/$1$3,$4,$2/mgx;
	}

	return $dump;
}

=pod

=back

=cut

1;
