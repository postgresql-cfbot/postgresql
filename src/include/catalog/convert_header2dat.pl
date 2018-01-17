#!/usr/bin/perl -w
#----------------------------------------------------------------------
#
# convert_header2dat.pl
#    Perl script that parses the catalog header files for BKI
#    DATA() and (SH)DESCR() statements, as well as defined symbols
#    referring to OIDs, and writes them out as native perl data
#    structures. White space and header commments referring to DATA()
#    lines are preserved. Some functions are loosely copied from
#    src/backend/catalog/Catalog.pm, whose equivalents have been
#    removed.
#
# Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# /src/include/catalog/convert_header2dat.pl
#
#----------------------------------------------------------------------

use strict;
use warnings;

use Data::Dumper;
# No $VARs - we add our own later.
$Data::Dumper::Terse = 1;

my @input_files;
my $output_path = '';
my $major_version;

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
	else
	{
		usage();
	}
}

# Sanity check arguments.
die "No input files.\n" if !@input_files;
foreach my $input_file (@input_files)
{
	if ($input_file !~ /\.h$/)
	{
		die "Input files need to be header files.\n";
	}
}

# Make sure output_path ends in a slash.
if ($output_path ne '' && substr($output_path, -1) ne '/')
{
	$output_path .= '/';
}

# Read all the input header files into internal data structures
# XXX This script is not robust against non-catalog headers. It's best
# to pass it the same list found in backend/catalog/Makefile.
my $catalogs = catalogs(@input_files);

# produce output, one catalog at a time
foreach my $catname (@{ $catalogs->{names} })
{
	my $catalog = $catalogs->{$catname};
	my $schema  = $catalog->{columns};

	# First, see if the header has any data entries. This is necessary
	# because headers with no DATA may still have comments that catalogs()
	# thought was in a DATA section.
	my $found_one = 0;
	foreach my $data (@{ $catalog->{data} })
	{
		if (ref $data eq 'HASH')
		{
			$found_one = 1;
		}
	}
	next if !$found_one;

	my @attnames;
	foreach my $column (@$schema)
	{
		my $attname = $column->{name};
		my $atttype = $column->{type};
		push @attnames, $attname;
	}

	my $datfile = "$output_path$catname.dat";
	open my $dat, '>', $datfile
	  or die "can't open $datfile: $!";

	# Write out data file.

	print $dat "# $catname.dat\n";
	print $dat "[\n\n";

	foreach my $data (@{ $catalog->{data} })
	{

		# Either a blank line or comment - just write it out.
		if (! ref $data)
		{
			print $dat "$data\n";
		}
		# Hash ref representing a data entry.
		elsif (ref $data eq 'HASH')
		{
			# Split line into tokens without interpreting their meaning.
			my %bki_values;
			@bki_values{@attnames} = split_data_line($data->{bki_values});

			# Flatten data hierarchy.
			delete $data->{bki_values};
			my %flat_data = (%$data, %bki_values);

			# Strip double quotes for readability. Most will be put
			# back in when writing postgres.bki
			foreach (values %flat_data)
			{
				s/"//g;
			}

			print $dat Dumper(\%flat_data);
			print $dat ",\n";
		}
	}

	print $dat "\n]\n";
}


# This function is a heavily modified version of its former namesake
# in Catalog.pm. There is possibly some dead code here. It's not worth
# removing.
sub catalogs
{
	my (%catalogs, $catname, $declaring_attributes, $most_recent);
	$catalogs{names} = [];

	# There are a few types which are given one name in the C source, but a
	# different name at the SQL level.  These are enumerated here.
	my %RENAME_ATTTYPE = (
		'int16'         => 'int2',
		'int32'         => 'int4',
		'int64'         => 'int8',
		'Oid'           => 'oid',
		'NameData'      => 'name',
		'TransactionId' => 'xid',
		'XLogRecPtr'    => 'pg_lsn');

	foreach my $input_file (@_)
	{
		my %catalog;
		$catalog{columns} = [];
		$catalog{data}    = [];
		my $is_varlen     = 0;
		my $saving_comments = 0;

		open(my $ifh, '<', $input_file) || die "$input_file: $!";
		my ($filename) = ($input_file =~ m/(\w+)\.h$/);

		# Skip these to keep the code simple.
		next if $filename eq 'toasting'
				or $filename eq 'indexing';

		# Scan the input file.
		while (<$ifh>)
		{
			# Determine whether we're in the DATA section and should
			# start saving header comments.
			if (/(\/|\s)\*\s+initial contents of pg_/)
			{
				$saving_comments = 1;
			}

			if ($saving_comments)
			{
				if ( m{^(/|\s+)\*\s+(.+?)(\*/)?$} )
				{
					my $comment = $2;

					# Filter out comments we know we don't want.
					if ($comment !~ /^-+$/
						and $comment !~ /initial contents of pg/
						and $comment !~ /PG_\w+_H/)
					{
						# Trim whitespace.
						$comment =~ s/^\s+//;
						$comment =~ s/\s+$//;
						push @{ $catalog{data} }, "# $comment";
					}
				}
				elsif (/^\s*$/)
				{
					# Preserve blank lines. Newline gets added by caller.
					push @{ $catalog{data} }, '';
				}
			}
			else
			{
				# Strip C-style comments.
				s;/\*(.|\n)*\*/;;g;
				if (m;/\*;)
				{
					# handle multi-line comments properly.
					my $next_line = <$ifh>;
					die "$input_file: ends within C-style comment\n"
					  if !defined $next_line;
					$_ .= $next_line;
					redo;
				}
			}

			# Strip useless whitespace and trailing semicolons.
			chomp;
			s/^\s+//;
			s/;\s*$//;
			s/\s+/ /g;

			# Push the data into the appropriate data structure.
			if (/^DATA\(insert(\s+OID\s+=\s+(\d+))?\s+\(\s*(.*)\s*\)\s*\)$/)
			{
				if ($2)
				{
					push @{ $catalog{data} }, { oid => $2, bki_values => $3 };
				}
				else
				{
					push @{ $catalog{data} }, { bki_values => $3 };
				}
			}
			# Save defined symbols referring to OIDs.
			elsif (/^#define\s+(\S+)\s+(\d+)$/)
			{
				$most_recent = $catalog{data}->[-1];
				my $oid_symbol = $1;

				# Print a warning if we find a defined symbol that is not
				# associated with the most recent DATA() statement, and is
				# not one of the symbols that we know to exclude.
				if (ref $most_recent ne 'HASH'
					and $oid_symbol !~ m/^Natts/
					and $oid_symbol !~ m/^Anum/
					and $oid_symbol !~ m/^STATISTIC_/
					and $oid_symbol !~ m/^TRIGGER_TYPE_/
					and $oid_symbol !~ m/RelationId$/
					and $oid_symbol !~ m/Relation_Rowtype_Id$/)
				{
					printf "Unhandled #define symbol: $filename: $_\n";
					next;
				}
				if (defined $most_recent->{oid} && $most_recent->{oid} ne $2)
				{
					print "#define does not apply to last seen oid \n$_\n";
					next;
				}
				$most_recent->{oid_symbol} = $oid_symbol;
			}
			elsif (/^DESCR\(\"(.*)\"\)$/)
			{
				$most_recent = $catalog{data}->[-1];

				# Test if most recent line is not a DATA() statement.
				if (ref $most_recent ne 'HASH')
				{
					die "DESCR() does not apply to any catalog ($input_file)";
				}
				if (!defined $most_recent->{oid})
				{
					die "DESCR() does not apply to any oid ($input_file)";
				}
				elsif ($1 ne '')
				{
					$most_recent->{descr} = $1;
				}
			}
			elsif (/^SHDESCR\(\"(.*)\"\)$/)
			{
				$most_recent = $catalog{data}->[-1];

				# Test if most recent line is not a DATA() statement.
				if (ref $most_recent ne 'HASH')
				{
					die "SHDESCR() does not apply to any catalog ($input_file)";
				}
				if (!defined $most_recent->{oid})
				{
					die "SHDESCR() does not apply to any oid ($input_file)";
				}
				elsif ($1 ne '')
				{
					$most_recent->{shdescr} = $1;
				}
			}
			elsif (/^CATALOG\(([^,]*),(\d+)\)/)
			{
				$catname = $1;
				$catalog{relation_oid} = $2;

				# Store pg_* catalog names in the same order we receive them
				push @{ $catalogs{names} }, $catname;

				$declaring_attributes = 1;
			}
			elsif ($declaring_attributes)
			{
				next if (/^{|^$/);
				next if (/^#/);
				if (/^}/)
				{
					undef $declaring_attributes;
				}
				else
				{
					my %column;
					my ($atttype, $attname, $attopt) = split /\s+/, $_;
					die "parse error ($input_file)" unless $attname;
					if (exists $RENAME_ATTTYPE{$atttype})
					{
						$atttype = $RENAME_ATTTYPE{$atttype};
					}
					if ($attname =~ /(.*)\[.*\]/)    # array attribute
					{
						$attname = $1;
						$atttype .= '[]';
					}

					$column{type} = $atttype;
					$column{name} = $attname;

					push @{ $catalog{columns} }, \%column;
				}
			}
		}
		if (defined $catname)
		{
			$catalogs{$catname} = \%catalog;
		}
		close $ifh;
	}
	return \%catalogs;
}

# Split a DATA line into fields.
# Call this on the bki_values element of a DATA item returned by catalogs();
# it returns a list of field values.  We don't strip quoting from the fields.
# Note: It should be safe to assign the result to a list of length equal to
# the nominal number of catalog fields, because the number of fields were
# checked in the original Catalog module.
sub split_data_line
{
	my $bki_values = shift;

	my @result = $bki_values =~ /"[^"]*"|\S+/g;
	return @result;
}

sub usage
{
	die <<EOM;
Usage: convert_macro2dat.pl [options] header...

Options:
    -o               output path

convert_macro2dat.pl generates data files from the same header files
currently parsed by Catalag.pm.

EOM
}
