#----------------------------------------------------------------------
#
# Catalog.pm
#    Perl module that extracts info from catalog files into Perl
#    data structures
#
# Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/backend/catalog/Catalog.pm
#
#----------------------------------------------------------------------

package Catalog;

use strict;
use warnings;

# Parses a catalog header file into a data structure describing the schema
# of the catalog.
sub ParseHeader
{
	my $input_file = shift;

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

	my $declaring_attributes;
	my %catalog;
	my $is_varlen     = 0;

	$catalog{columns} = [];
	$catalog{toasting} = [];
	$catalog{indexing} = [];

	open(my $ifh, '<', $input_file) || die "$input_file: $!";

	# Scan the input file.
	while (<$ifh>)
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

		# Strip useless whitespace and trailing semicolons.
		chomp;
		s/^\s+//;
		s/;\s*$//;
		s/\s+/ /g;

		# Push the data into the appropriate data structure.
		if (/^DECLARE_TOAST\(\s*(\w+),\s*(\d+),\s*(\d+)\)/)
		{
			my ($toast_name, $toast_oid, $index_oid) = ($1, $2, $3);
			push @{ $catalog{toasting} },
			  "declare toast $toast_oid $index_oid on $toast_name\n";
		}
		elsif (/^DECLARE_(UNIQUE_)?INDEX\(\s*(\w+),\s*(\d+),\s*(.+)\)/)
		{
			my ($is_unique, $index_name, $index_oid, $using) =
			  ($1, $2, $3, $4);
			push @{ $catalog{indexing} },
			  sprintf(
				"declare %sindex %s %s %s\n",
				$is_unique ? 'unique ' : '',
				$index_name, $index_oid, $using);
		}
		elsif (/^CATALOG\(([^,]*),(\d+)\)/)
		{
			$catalog{catname} = $1;
			$catalog{relation_oid} = $2;

			$catalog{bootstrap} = /BKI_BOOTSTRAP/ ? ' bootstrap' : '';
			$catalog{shared_relation} =
			  /BKI_SHARED_RELATION/ ? ' shared_relation' : '';
			$catalog{without_oids} =
			  /BKI_WITHOUT_OIDS/ ? ' without_oids' : '';
			$catalog{rowtype_oid} =
			  /BKI_ROWTYPE_OID\((\d+)\)/ ? " rowtype_oid $1" : '';
			$catalog{schema_macro} = /BKI_SCHEMA_MACRO/ ? 1 : 0;
			$declaring_attributes = 1;
		}
		elsif ($declaring_attributes)
		{
			next if (/^{|^$/);
			if (/^#/)
			{
				$is_varlen = 1 if /^#ifdef\s+CATALOG_VARLEN/;
				next;
			}
			if (/^}/)
			{
				undef $declaring_attributes;
			}
			else
			{
				my %column;
				my @attopts = split /\s+/, $_;
				my $atttype = shift @attopts;
				my $attname = shift @attopts;
				die "parse error ($input_file)"
				  unless ($attname and $atttype);

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
				$column{is_varlen} = 1 if $is_varlen;

				foreach my $attopt (@attopts)
				{
					if ($attopt eq 'BKI_FORCE_NULL')
					{
						$column{forcenull} = 1;
					}
					elsif ($attopt eq 'BKI_FORCE_NOT_NULL')
					{
						$column{forcenotnull} = 1;
					}
					elsif ($attopt =~ /BKI_DEFAULT\((\S+)\)/)
					{
						$column{default} = $1;
					}
					elsif ($attopt =~ /BKI_ABBREV\((\S+)\)/)
					{
						$column{abbrev} = $1;
					}
					else
					{
						die "unknown column option $attopt on column $attname";
					}

					if ($column{forcenull} and $column{forcenotnull})
					{
						die "$attname is forced both null and not null";
					}
				}
				push @{ $catalog{columns} }, \%column;
			}
		}
	}
	close $ifh;
	return \%catalog;
}

# Parses a file containing Perl data structure literals, returning live data.
#
# The parameter $preserve_formatting needs to be set for callers that want
# to work with non-data lines in the data files, such as comments and blank
# lines. If a caller just wants consume the data, leave it unset.
sub ParseData
{
	my ($input_file, $schema, $preserve_formatting) = @_;

	open(my $ifh, '<', $input_file) || die "$input_file: $!";
	$input_file =~ /(\w+)\.dat$/;
	my $catname = $1;
	my $data = [];
	my $prev_blank = 0;

	# Scan the input file.
	while (<$ifh>)
	{
		my $datum;

		if (/^\s*$/)
		{
			# Preserve non-consecutive blank lines.
			# Newline gets added by caller.
			next if $prev_blank;
			$datum = '';
			$prev_blank = 1;
		}
		else
		{
			$prev_blank = 0;
		}

		if (/{/)
		{
			# Capture the hash ref
			# NB: Assumes that the next hash ref can't start on the
			# same line where the present one ended.
			# Not foolproof, but we shouldn't need a full parser,
			# since we expect relatively well-behaved input.

			# Quick hack to detect when we have a full hash ref to
			# parse. We can't just use a regex because of values in
			# pg_aggregate and pg_proc like '{0,0}'.
			my $lcnt = tr/{//;
			my $rcnt = tr/}//;

			if ($lcnt == $rcnt)
			{
				eval '$datum = ' . $_;
				if (!ref $datum)
				{
					die "Error parsing $_\n$!";
				}

				# Expand tuples to their full representation.
				# We must do the following operations in the order given.
				resolve_column_abbrevs($datum, $schema);

				if ($catname eq 'pg_proc')
				{
					compute_pg_proc_fields($datum);
				}
				elsif ($catname eq 'pg_type' and !exists $datum->{oid_symbol})
				{
					my $symbol = GetPgTypeSymbol($datum->{typname});
					$datum->{oid_symbol} = $symbol
					  if defined $symbol;
				}

				my $error = AddDefaultValues($datum, $schema);
				if ($error)
				{
					print "Failed to form full tuple for $catname\n";
					die $error;
				}
			}
			else
			{
				my $next_line = <$ifh>;
				die "$input_file: ends within Perl hash\n"
				  if !defined $next_line;
				$_ .= $next_line;
				redo;
			}
		}
		# Capture comments that are on their own line.
		elsif (/^\s*#\s*(.+)\s*/)
		{
			$datum = "# $1";
		}
		# Assume bracket is the only token in the line.
		elsif (/^\s*(\[|\])\s*$/)
		{
			$datum = $1;
		}

		next if !defined $datum;

		# Hash references are data, so always push.
		# Other datums are non-data strings, so only push if we
		# want formatting.
		if ($preserve_formatting or ref $datum eq 'HASH')
		{
			push @$data, $datum;
		}
	}
	return $data;
}

# Copy values from abbreviated keys to full keys.
sub resolve_column_abbrevs
{
	my $row    = shift;
	my $schema = shift;

	foreach my $column (@$schema)
	{
		my $abbrev  = $column->{abbrev};
		my $attname = $column->{name};
		if (defined $abbrev and defined $row->{$abbrev})
		{
			$row->{$attname} = $row->{$abbrev};
		}
	}
}

# Fill in default values of a record using the given schema. It's the
# caller's responsibility to specify other values beforehand.
sub AddDefaultValues
{
	my ($row, $schema) = @_;
	my @missing_fields;
	my $msg;

	foreach my $column (@$schema)
	{
		my $attname = $column->{name};
		my $atttype = $column->{type};

		if (defined $row->{$attname})
		{
			;
		}
		elsif (defined $column->{default})
		{
			$row->{$attname} = $column->{default};
		}
		else
		{
			# Failed to find a value.
			push @missing_fields, $attname;
		}
	}

	if (@missing_fields)
	{
		$msg = "Missing values for: " . join(', ', @missing_fields);
		$msg .= "\nShowing other values for context:\n";
		while (my($key, $value) = each %$row)
		{
			$msg .= "$key => $value, ";
		}
	}
	return $msg;
}

# Compute certain pg_proc fields from others.
sub compute_pg_proc_fields
{
	my $row = shift;

	# pronargs is computed by counting proargtypes.
	if ($row->{proargtypes})
	{
		my @argtypes = split /\s+/, $row->{proargtypes};
		$row->{pronargs} = scalar(@argtypes);
	}
	else
	{
		$row->{pronargs} = '0';
	}

	# If prosrc doesn't exist, it must be a copy of proname.
	if (!exists $row->{prosrc})
	{
		$row->{prosrc} = $row->{proname}
	}
}

# Determine canonical pg_type OID #define symbol from the type name.
sub GetPgTypeSymbol
{
	my $typename = shift;

	# Skip for rowtypes of bootstrap tables.
	return
	  if $typename eq 'pg_type'
	    or $typename eq 'pg_proc'
	    or $typename eq 'pg_attribute'
	    or $typename eq 'pg_class';

	$typename =~ /(_)?(.+)/;
	my $arraystr = $1 ? 'ARRAY' : '';
	my $name = uc $2;
	return $name . $arraystr . 'OID';
}

# Rename temporary files to final names.
# Call this function with the final file name and the .tmp extension
# Note: recommended extension is ".tmp$$", so that parallel make steps
# can't use the same temp files
sub RenameTempFile
{
	my $final_name = shift;
	my $extension  = shift;
	my $temp_name  = $final_name . $extension;
	print "Writing $final_name\n";
	rename($temp_name, $final_name) || die "rename: $temp_name: $!";
}

# Find a symbol defined in a particular header file and extract the value.
#
# The include path has to be passed as a reference to an array.
sub FindDefinedSymbol
{
	my ($catalog_header, $include_path, $symbol) = @_;

	for my $path (@$include_path)
	{

		# Make sure include path ends in a slash.
		if (substr($path, -1) ne '/')
		{
			$path .= '/';
		}
		my $file = $path . $catalog_header;
		next if !-f $file;
		open(my $find_defined_symbol, '<', $file) || die "$file: $!";
		while (<$find_defined_symbol>)
		{
			if (/^#define\s+\Q$symbol\E\s+(\S+)/)
			{
				return $1;
			}
		}
		close $find_defined_symbol;
		die "$file: no definition found for $symbol\n";
	}
	die "$catalog_header: not found in any include directory\n";
}

sub FindDefinedSymbolFromData
{
	my ($data, $symbol) = @_;
	foreach my $row (@{ $data })
	{
		if ($row->{oid_symbol} eq $symbol)
		{
			return $row->{oid};
		}
		die "no definition found for $symbol\n";
	}
}

1;
