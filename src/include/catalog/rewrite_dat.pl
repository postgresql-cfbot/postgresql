#!/usr/bin/perl -w
#----------------------------------------------------------------------
#
# rewrite_dat.pl
#    Perl script that reads in a catalog data file and writes out
#    a functionally equivalent file in a standard format.
#
#    -Metadata fields are on their own line
#    -Fields are in the same order they would be in the catalog table
#    -Default values and computed values for the catalog are left out.
#    -Column abbreviations are used if available.
#
# Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# /src/include/catalog/rewrite_dat.pl
#
#----------------------------------------------------------------------

use Catalog;

use strict;
use warnings;

my @input_files;
my $output_path = '';
my $expand_tuples = 0;

# Process command line switches.
while (@ARGV)
{
	my $arg = shift @ARGV;
	if ($arg !~ /^-/)
	{
		push @input_files, $arg;
	}
	elsif ($arg =~ /^-o/)
	{
		$output_path = length($arg) > 2 ? substr($arg, 2) : shift @ARGV;
	}
	elsif ($arg eq '--revert')
	{
		revert();
	}
	elsif ($arg eq '--expand')
	{
		$expand_tuples = 1;
	}
	else
	{
		usage();
	}
}

# Sanity check arguments.
die "No input files.\n"
  if !@input_files;

# Make sure output_path ends in a slash.
if ($output_path ne '' && substr($output_path, -1) ne '/')
{
	$output_path .= '/';
}

# Metadata of a catalog entry
my @metafields = ('oid', 'oid_symbol', 'descr', 'shdescr');

# Read all the input files into internal data structures.
# We pass data file names as arguments and then look for matching
# headers to parse the schema from.
foreach my $datfile (@input_files)
{
	$datfile =~ /(.+)\.dat$/
	  or die "Input files need to be data (.dat) files.\n";

	my $header = "$1.h";
	die "There in no header file corresponding to $datfile"
	  if ! -e $header;

	my @attnames;
	my $catalog = Catalog::ParseHeader($header);
	my $catname = $catalog->{catname};
	my $schema  = $catalog->{columns};

	foreach my $column (@$schema)
	{
		my $attname;

		# Use abbreviations where available, unless we're writing
		# full tuples.
		if (exists $column->{abbrev} and !$expand_tuples)
		{
			$attname = $column->{abbrev};
		}
		else
		{
			$attname = $column->{name};
		}
		push @attnames, $attname;
	}

	my $catalog_data = Catalog::ParseData($datfile, $schema, 1);
	next if !defined $catalog_data;

	# Back up old data file rather than overwrite it.
	# We don't assume the input path and output path are the same,
	# but they can be.
	my $newdatfile = "$output_path$catname.dat";
	if (-e $newdatfile)
	{
		rename($newdatfile, $newdatfile . '.bak')
		  or die "rename: $newdatfile: $!";
	}
	open my $dat, '>', $newdatfile
	  or die "can't open $newdatfile: $!";

	# Write the data.
	foreach my $data (@$catalog_data)
	{
		# Either a newline, comment, or bracket - just write it out.
		if (! ref $data)
		{
			print $dat "$data\n";
		}
		# Hash ref representing a data entry.
		elsif (ref $data eq 'HASH')
		{
			my %values = %$data;
			print $dat "{ ";

			# Write out tuples in a compact representation. We must do
			# these operations in the order given.
			# Note: This is also a convenient place to do one-off
			# bulk-editing.
			if (!$expand_tuples)
			{
				strip_default_values(\%values, $schema, $catname);

				# Delete values that are computable from other fields.
				if ($catname eq 'pg_proc')
				{
					delete $values{pronargs};
					delete $values{prosrc}
					  if $values{prosrc} eq $values{proname};
				}
				elsif ($catname eq 'pg_type' and exists $values{oid_symbol})
				{
					my $symbol = Catalog::GetPgTypeSymbol($values{typname});
					delete $values{oid_symbol}
					  if defined $symbol
					    and $values{oid_symbol} eq $symbol;
				}

				add_column_abbrevs(\%values, $schema);
			}

			# Separate out metadata fields for readability.
			my $metadata_line = format_line(\%values, @metafields);
			if ($metadata_line)
			{
				print $dat $metadata_line;
				print $dat ",\n";
			}
			my $data_line = format_line(\%values, @attnames);

			# Line up with metadata line, if there is one.
			if ($metadata_line)
			{
				print $dat '  ';
			}
			print $dat $data_line;
			print $dat " },\n";
		}
		else
		{
			die "Unexpected data type";
		}
	}
}

sub strip_default_values
{
	my ($row, $schema, $catname) = @_;

	foreach my $column (@$schema)
	{
		my $attname = $column->{name};
		die "No value for $catname.$attname\n"
		  if ! defined $row->{$attname};

		# Delete values that match defaults.
		if (defined $column->{default}
			and ($row->{$attname} eq $column->{default}))
		{
			delete $row->{$attname};
		}
	}
}

sub add_column_abbrevs
{
	my $row    = shift;
	my $schema = shift;

	foreach my $column (@$schema)
	{
		my $abbrev  = $column->{abbrev};
		my $attname = $column->{name};
		if (defined $abbrev and exists $row->{$attname})
		{
			$row->{$abbrev} = $row->{$attname};
		}
	}
}

sub format_line
{
	my $data = shift;
	my @atts = @_;

	my $first = 1;
	my $value;
	my $line = '';

	foreach my $field (@atts)
	{
		next if !defined $data->{$field};
		$value = $data->{$field};

		# Re-escape single quotes.
		$value =~ s/'/\\'/g;

		if (!$first)
		{
			$line .= ', ';
		}
		$first = 0;

		$line .= "$field => '$value'";
	}
	return $line;
}

# Rename .bak files back to .dat
# This requires passing the .dat files as arguments to the script as normal.
sub revert
{
	foreach my $datfile (@input_files)
	{
		my $bakfile = "$datfile.bak";
		if (-e $bakfile)
		{
			rename($bakfile, $datfile) or die "rename: $bakfile: $!";
		}
	}
	exit 0;
}

sub usage
{
	die <<EOM;
Usage: rewrite_dat.pl [options] datafile...

Options:
    -o               output path
    --expand         write out full tuples
    --revert         rename .bak files back to .dat

Expects a list of .dat files as arguments.

Make sure location of Catalog.pm is passed to the perl interpreter:
perl -I /path/to/Catalog.pm/ ...

EOM
}
