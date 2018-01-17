#!/usr/bin/perl -w
#----------------------------------------------------------------------
#
# genbki.pl
#    Perl script that generates postgres.bki, postgres.description,
#    postgres.shdescription, and schemapg.h from specially formatted
#    header files and data files.  The BKI files are used to initialize
#    the postgres template database.
#
# Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/backend/catalog/genbki.pl
#
#----------------------------------------------------------------------

use Catalog;

use strict;
use warnings;

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
	elsif ($arg =~ /^--set-version=(.*)$/)
	{
		$major_version = $1;
		die "Invalid version string.\n"
		  if !($major_version =~ /^\d+$/);
	}
	else
	{
		usage();
	}
}

# Sanity check arguments.
die "No input files.\n"                  if !@input_files;
die "--set-version must be specified.\n" if !defined $major_version;

# Make sure output_path ends in a slash.
if ($output_path ne '' && substr($output_path, -1) ne '/')
{
	$output_path .= '/';
}

# Open temp files
my $tmpext  = ".tmp$$";
my $bkifile = $output_path . 'postgres.bki';
open my $bki, '>', $bkifile . $tmpext
  or die "can't open $bkifile$tmpext: $!";
my $schemafile = $output_path . 'schemapg.h';
open my $schemapg, '>', $schemafile . $tmpext
  or die "can't open $schemafile$tmpext: $!";
my $descrfile = $output_path . 'postgres.description';
open my $descr, '>', $descrfile . $tmpext
  or die "can't open $descrfile$tmpext: $!";
my $shdescrfile = $output_path . 'postgres.shdescription';
open my $shdescr, '>', $shdescrfile . $tmpext
  or die "can't open $shdescrfile$tmpext: $!";
my $symbolfile = $output_path . 'oid_symbols.h';
open my $symbol, '>', $symbolfile . $tmpext
  or die "can't open $symbolfile$tmpext: $!";

# Read all the files into internal data structures. Not all catalogs
# will have a data file.
my @catnames;
my %catalogs;
my %catalog_data;
my @toast_decls;
my @index_decls;
foreach my $header (@input_files)
{
	$header =~ /(.+)\.h$/
	  or die "Input files need to be header files.\n";
	my $datfile = "$1.dat";

	my $catalog = Catalog::ParseHeader($header);
	my $catname = $catalog->{catname};
	my $schema  = $catalog->{columns};

	push @catnames, $catname;
	$catalogs{$catname} = $catalog;

	if (-e $datfile)
	{
		$catalog_data{$catname} = Catalog::ParseData($datfile, $schema, 0);
	}

	foreach my $toast_decl (@{ $catalog->{toasting} })
	{
		push @toast_decls, $toast_decl;
	}
	foreach my $index_decl (@{ $catalog->{indexing} })
	{
		push @index_decls, $index_decl;
	}
}

# Fetch some special data that we will substitute into the output file.
# CAUTION: be wary about what symbols you substitute into the .bki file here!
# It's okay to substitute things that are expected to be really constant
# within a given Postgres release, such as fixed OIDs.  Do not substitute
# anything that could depend on platform or configuration.  (The right place
# to handle those sorts of things is in initdb.c's bootstrap_template1().)
my $BOOTSTRAP_SUPERUSERID =
  Catalog::FindDefinedSymbolFromData($catalog_data{pg_authid}, 'BOOTSTRAP_SUPERUSERID');
my $PG_CATALOG_NAMESPACE =
  Catalog::FindDefinedSymbolFromData($catalog_data{pg_namespace}, 'PG_CATALOG_NAMESPACE');

# Build lookup tables for reg* substitutions and for pg_attribute
# copies of pg_type values.

# procedure OID lookup
my %regprocoids;
foreach my $row (@{ $catalog_data{pg_proc} })
{
	if (defined($regprocoids{ $row->{proname} }))
	{
		$regprocoids{ $row->{proname} } = 'MULTIPLE';
	}
	else
	{
		$regprocoids{ $row->{proname} } = $row->{oid};
	}
}

# index access method OID lookup
my %regamoids;
foreach my $row (@{ $catalog_data{pg_am} })
{
	$regamoids{$row->{amname}} = $row->{oid};
}

# operator OID lookup
my %regoperoids;
foreach my $row (@{ $catalog_data{pg_operator} })
{
	# There is no unique name, so we need to invent one that contains
	# the relevant type names.
	my $key = sprintf "%s(%s,%s)",
	  $row->{oprname}, $row->{oprleft}, $row->{oprright};
	$regoperoids{$key} = $row->{oid};
}

# opfamily OID lookup
my %regopfoids;
foreach my $row (@{ $catalog_data{pg_opfamily} })
{
	# There is no unique name, so we need to combine access method
	# and opfamily name.
	my $key = sprintf "%s/%s",
	  $row->{opfmethod}, $row->{opfname};
	$regopfoids{$key} = $row->{oid};
}

# type lookups
my %regtypeoids;
my %types;
foreach my $row (@{ $catalog_data{pg_type} })
{
	$regtypeoids{$row->{typname}} = $row->{oid};
	$types{ $row->{typname} } = $row;
}

# We use OID aliases to indicate when to do OID lookups, so these types
# have to be turned back into 'oid' before writing the CREATE command.
my %RENAME_REGOID = (
	regam => 'oid',
	regoper => 'oid',
	regopf => 'oid',
	regtype => 'oid');

# Generate postgres.bki, postgres.description, and postgres.shdescription

# version marker for .bki file
print $bki "# PostgreSQL $major_version\n";

# vars to hold data needed for schemapg.h
my %schemapg_entries;
my @tables_needing_macros;

# var to hold data for oid_symbols.h
my %oid_symbols;
my @tables_with_oid_symbols;

# produce output, one catalog at a time
foreach my $catname (@catnames)
{

	# .bki CREATE command for this catalog
	my $catalog = $catalogs{$catname};
	print $bki "create $catname $catalog->{relation_oid}"
	  . $catalog->{shared_relation}
	  . $catalog->{bootstrap}
	  . $catalog->{without_oids}
	  . $catalog->{rowtype_oid} . "\n";

	my @attnames;
	my $first = 1;

	print $bki " (\n";
	my $schema = $catalog->{columns};
	foreach my $column (@$schema)
	{
		my $attname = $column->{name};
		push @attnames, $attname;

		my $atttype = $column->{type};
		$atttype = $RENAME_REGOID{$atttype}
		  if exists $RENAME_REGOID{$atttype};

		if (!$first)
		{
			print $bki " ,\n";
		}
		$first = 0;

		print $bki " $attname = $atttype";

		if (defined $column->{forcenotnull})
		{
			print $bki " FORCE NOT NULL";
		}
		elsif (defined $column->{forcenull})
		{
			print $bki " FORCE NULL";
		}
	}
	print $bki "\n )\n";

	# Open it, unless bootstrap case (create bootstrap does this
	# automatically)
	if (!$catalog->{bootstrap})
	{
		print $bki "open $catname\n";
	}

	if ($catname eq 'pg_attribute')
	{
		gen_pg_attribute($schema, @attnames);
	}

	# Ordinary catalog with a data file
	foreach my $row (@{ $catalog_data{$catname} })
	{
		my %bki_values = %$row;

		# Perform required substitutions on fields
		foreach my $column (@$schema)
		{
			my $attname = $column->{name};
			my $atttype = $column->{type};

			# Substitute constant values we acquired above.
			# (It's intentional that this can apply to parts of a field).
			$bki_values{$attname} =~ s/\bPGUID\b/$BOOTSTRAP_SUPERUSERID/g;
			$bki_values{$attname} =~ s/\bPGNSP\b/$PG_CATALOG_NAMESPACE/g;

			# Replace reg* columns' values with OIDs.
			if ($atttype eq 'regproc')
			{
				# If we don't have a unique value to substitute,
				# just do nothing (regprocin will complain).
				my $procoid = $regprocoids{ $bki_values{$attname} };
				$bki_values{$attname} = $procoid
				  if defined($procoid) && $procoid ne 'MULTIPLE';
			}
			elsif ($atttype eq 'regam')
			{
				my $amoid = $regamoids{ $bki_values{$attname} };
				$bki_values{$attname} = $amoid
				  if defined($amoid);
			}
			elsif ($atttype eq 'regopf')
			{
				my $opfoid = $regopfoids{ $bki_values{$attname} };
				$bki_values{$attname} = $opfoid
				  if defined($opfoid);
			}
			elsif ($atttype eq 'regoper')
			{
				my $operoid = $regoperoids{ $bki_values{$attname} };
				$bki_values{$attname} = $operoid
				  if defined($operoid);
			}
			elsif ($atttype eq 'regtype')
			{
				my $typeoid = $regtypeoids{ $bki_values{$attname} };
				$bki_values{$attname} = $typeoid
				  if defined($typeoid);
			}
		}

		# We can't do regtype lookups in a general way for
		# pg_proc, so do special handling here.
		if ($catname eq 'pg_proc')
		{

			# prorettype
			# Note: We could handle this automatically by using the
			# 'regtype' alias, but then we would have to teach
			# emit_pgattr_row() to change the attribute type back to
			# oid. Since we have to treat pg_proc differently anyway,
			# just do the type lookup manually here.
			my $rettypeoid = $regtypeoids{ $bki_values{prorettype}};
			$bki_values{prorettype} = $rettypeoid
			  if defined($rettypeoid);

			# proargtypes
			if ($bki_values{proargtypes})
			{
				my @argtypenames = split /\s+/, $bki_values{proargtypes};
				my @argtypeoids;
				foreach my $argtypename (@argtypenames)
				{
					my $argtypeoid  = $regtypeoids{$argtypename};
					push @argtypeoids, $argtypeoid;
				}
				$bki_values{proargtypes} = join(' ', @argtypeoids);
			}

			# proallargtypes
			if ($bki_values{proallargtypes} ne '_null_')
			{
				$bki_values{proallargtypes} =~ s/[{}]//g;
				my @argtypenames = split /,/, $bki_values{proallargtypes};
				my @argtypeoids;
				foreach my $argtypename (@argtypenames)
				{
					my $argtypeoid  = $regtypeoids{$argtypename};
					push @argtypeoids, $argtypeoid;
				}
				$bki_values{proallargtypes} =
					'{' . join(',', @argtypeoids) . '}';
			}
		}

		# Store OID symbols for later.
		if (exists $bki_values{oid_symbol})
		{
			if (!@tables_with_oid_symbols
				or $tables_with_oid_symbols[-1] ne $catname)
			{
				push @tables_with_oid_symbols, $catname;
				$oid_symbols{$catname} = [];
			}

			push @{ $oid_symbols{$catname} },
			  sprintf "#define %s %s",
				$bki_values{oid_symbol}, $bki_values{oid};
		}

		# Add quotes where necessary.
		quote_bki_values(\%bki_values, $schema);

		# Write to postgres.bki
		print_bki_insert(\%bki_values, @attnames);

		# Write comments to postgres.description and
		# postgres.shdescription
		if (defined $bki_values{descr})
		{
			printf $descr "%s\t%s\t0\t%s\n",
			  $bki_values{oid}, $catname, $bki_values{descr};
		}
		if (defined $bki_values{shdescr})
		{
			printf $shdescr "%s\t%s\t%s\n",
			  $bki_values{oid}, $catname, $bki_values{shdescr};
		}
	}
	print $bki "close $catname\n";
}

# Any information needed for the BKI that is not contained in a pg_*.h header
# (i.e., not contained in a header with a CATALOG() statement) comes here

# Write out declare toast/index statements. Index commands must be last,
# since the indexes can't be created until all the tables are in place,
# and toast commands are next-to-last to make sure they are run after
# creating all the tables that need toast tables.
foreach my $declaration (@toast_decls)
{
	print $bki $declaration;
}

foreach my $declaration (@index_decls)
{
	print $bki $declaration;
}
print $bki "build indices\n";


# Now generate schemapg.h

# Opening boilerplate for schemapg.h
print $schemapg <<EOM;
/*-------------------------------------------------------------------------
 *
 * schemapg.h
 *    Schema_pg_xxx macros for use by relcache.c
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES
 *  ******************************
 *  *** DO NOT EDIT THIS FILE! ***
 *  ******************************
 *
 *  It has been GENERATED by src/backend/catalog/genbki.pl
 *
 *-------------------------------------------------------------------------
 */
#ifndef SCHEMAPG_H
#define SCHEMAPG_H
EOM

# Emit schemapg declarations
foreach my $table_name (@tables_needing_macros)
{
	print $schemapg "\n#define Schema_$table_name \\\n";
	print $schemapg join ", \\\n", @{ $schemapg_entries{$table_name} };
	print $schemapg "\n";
}

# Closing boilerplate for schemapg.h
print $schemapg "\n#endif /* SCHEMAPG_H */\n";

# Generate oid_symbols.h

# Opening boilerplate for oid_symbol.h
print $symbol <<EOM;
/*-------------------------------------------------------------------------
 *
 * oid_symbols.h
 *    Oid symbols to be included in catalog headers.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES
 *  ******************************
 *  *** DO NOT EDIT THIS FILE! ***
 *  ******************************
 *
 *  It has been GENERATED by src/backend/catalog/genbki.pl
 *
 *-------------------------------------------------------------------------
 */
#ifndef OID_SYMBOLS_H
#define OID_SYMBOLS_H
EOM

# Emit oid symbols
foreach my $table_name (@tables_with_oid_symbols)
{
	print $symbol "\n\n/* $table_name */\n";
	print $symbol join "\n", @{ $oid_symbols{$table_name} };
}

# Closing boilerplate for oid_symbols.h
print $symbol "\n\n#endif /* OID_SYMBOLS_H */\n";

# We're done emitting data
close $bki;
close $schemapg;
close $descr;
close $shdescr;
close $symbol;

# Finally, rename the completed files into place.
Catalog::RenameTempFile($bkifile,     $tmpext);
Catalog::RenameTempFile($schemafile,  $tmpext);
Catalog::RenameTempFile($descrfile,   $tmpext);
Catalog::RenameTempFile($shdescrfile, $tmpext);
Catalog::RenameTempFile($symbolfile,  $tmpext);

exit 0;

#################### Subroutines ########################

# Supply quoting for a normal bki row.
# This allows us to keep most double quotes
# out of the catalog data files for readability.
sub quote_bki_values
{
	my $row    = shift;
	my $schema = shift;

	foreach my $column (@$schema)
	{
		my $attname = $column->{name};
		my $atttype = $column->{type};

		if
		(
			length($row->{$attname}) == 0  # Empty string
			or $row->{$attname} =~ /\s/    # Whitespace

			# Quote strings that have special characters
			# except for certain cases. See bootscanner.l
			or (    $row->{$attname} =~ /\W/
				and $row->{$attname} !~ /^\\\d{3}$/  # octal
				and $row->{$attname} !~ /^-\d*$/)    # '-' or '-1'

			# XXX Not needed, but keeps the .bki diff down to a reasonable
			# size during review
			or $attname eq 'oprname'    # Operator names
			or $atttype eq 'oidvector'  # Arrays etc.
			or $atttype eq 'int2vector'
			or $atttype =~ /\[\]$/
		)
		{
			if ($row->{$attname} ne '_null_' and $row->{$attname} !~ /^"([^"])*"$/)
			{
				$row->{$attname} = q|"| . $row->{$attname} . q|"|;
			}
		}
	}
}

# For pg_attribute.h, we generate data entries ourselves.
sub gen_pg_attribute
{
	my $schema = shift;
	my @attnames = @_;

	foreach my $table_name (@catnames)
	{
		my $table = $catalogs{$table_name};

		# Currently, all bootstrapped relations also need schemapg.h
		# entries, so skip if the relation isn't to be in schemapg.h.
		next if !$table->{schema_macro};

		$schemapg_entries{$table_name} = [];
		push @tables_needing_macros, $table_name;

		# Generate entries for user attributes.
		my $attnum       = 0;
		my $priornotnull = 1;
		foreach my $attr (@{ $table->{columns} })
		{
			$attnum++;
			my %row;
			$row{attnum}   = $attnum;
			$row{attrelid} = $table->{relation_oid};

			morph_row_for_pgattr(\%row, $schema, $attr, $priornotnull);
			$priornotnull &= ($row{attnotnull} eq 't');

			# If it's bootstrapped, put an entry in postgres.bki.
			print_bki_insert(\%row, @attnames) if $table->{bootstrap};

			# Store schemapg entries for later.
			morph_row_for_schemapg(\%row, $schema);
			push @{ $schemapg_entries{$table_name} },
			  sprintf "{ %s }",
				join(', ', grep { defined $_ } @row{@attnames});
		}

		# Generate entries for system attributes.
		# We only need postgres.bki entries, not schemapg.h entries.
		if ($table->{bootstrap})
		{
			$attnum = 0;
			my @SYS_ATTRS = (
				{ name => 'ctid',     type => 'tid' },
				{ name => 'oid',      type => 'oid' },
				{ name => 'xmin',     type => 'xid' },
				{ name => 'cmin',     type => 'cid' },
				{ name => 'xmax',     type => 'xid' },
				{ name => 'cmax',     type => 'cid' },
				{ name => 'tableoid', type => 'oid' });
			foreach my $attr (@SYS_ATTRS)
			{
				$attnum--;
				my %row;
				$row{attnum}        = $attnum;
				$row{attrelid}      = $table->{relation_oid};
				$row{attstattarget} = '0';

				# Omit the oid column if the catalog doesn't have them
				next
				  if $table->{without_oids}
					  && $attr->{name} eq 'oid';

				morph_row_for_pgattr(\%row, $schema, $attr, 1);
				print_bki_insert(\%row, @attnames);
			}
		}
	}
}

# Given $pgattr_schema (the pg_attribute schema for a catalog sufficient for
# AddDefaultValues), $attr (the description of a catalog row), and
# $priornotnull (whether all prior attributes in this catalog are not null),
# modify the $row hashref for print_bki_insert.  This includes setting data
# from the corresponding pg_type element and filling in any default values.
# Any value not handled here must be supplied by caller.
sub morph_row_for_pgattr
{
	my ($row, $pgattr_schema, $attr, $priornotnull) = @_;
	my $attname = $attr->{name};
	my $atttype = $attr->{type};

	$row->{attname} = $attname;

	# Adjust type name for arrays: foo[] becomes _foo, so we can look it up in
	# pg_type
	$atttype = '_' . $1 if $atttype =~ /(.+)\[\]$/;

	# Copy the type data from pg_type, and add some type-dependent items
	my $type = $types{$atttype};

	$row->{atttypid}   = $type->{oid};
	$row->{attlen}     = $type->{typlen};
	$row->{attbyval}   = $type->{typbyval};
	$row->{attstorage} = $type->{typstorage};
	$row->{attalign}   = $type->{typalign};

	# set attndims if it's an array type
	$row->{attndims} = $type->{typcategory} eq 'A' ? '1' : '0';
	$row->{attcollation} = $type->{typcollation};

	if (defined $attr->{forcenotnull})
	{
		$row->{attnotnull} = 't';
	}
	elsif (defined $attr->{forcenull})
	{
		$row->{attnotnull} = 'f';
	}
	elsif ($priornotnull)
	{

		# attnotnull will automatically be set if the type is
		# fixed-width and prior columns are all NOT NULL ---
		# compare DefineAttr in bootstrap.c. oidvector and
		# int2vector are also treated as not-nullable.
		$row->{attnotnull} =
		$type->{typname} eq 'oidvector'   ? 't'
		: $type->{typname} eq 'int2vector'  ? 't'
		: $type->{typlen}  eq 'NAMEDATALEN' ? 't'
		: $type->{typlen} > 0 ? 't'
		:                       'f';
	}
	else
	{
		$row->{attnotnull} = 'f';
	}

	my $error = Catalog::AddDefaultValues($row, $pgattr_schema);
	if ($error)
	{
		die "Failed to form full tuple for pg_attribute: ", $error;
	}
}

# Write a pg_attribute entry to postgres.bki
sub print_bki_insert
{
	my $row        = shift;
	my @attnames   = @_;
	my $oid        = $row->{oid} ? "OID = $row->{oid} " : '';
	my $bki_values = join ' ', @{$row}{@attnames};
	printf $bki "insert %s( %s )\n", $oid, $bki_values;
}

# Given a row reference, modify it so that it becomes a valid entry for
# a catalog schema declaration in schemapg.h.
#
# The field values of a Schema_pg_xxx declaration are similar, but not
# quite identical, to the corresponding values in postgres.bki.
sub morph_row_for_schemapg
{
	my $row           = shift;
	my $pgattr_schema = shift;

	foreach my $column (@$pgattr_schema)
	{
		my $attname = $column->{name};
		my $atttype = $column->{type};

		# Some data types have special formatting rules.
		if ($atttype eq 'name')
		{
			# add {" ... "} quoting
			$row->{$attname} = sprintf(qq'{"%s"}', $row->{$attname});
		}
		elsif ($atttype eq 'char')
		{
			# Replace empty string by zero char constant; add single quotes
			$row->{$attname} = '\0' if $row->{$attname} eq q|""|;
			$row->{$attname} = sprintf("'%s'", $row->{$attname});
		}

		# Expand booleans from 'f'/'t' to 'false'/'true'.
		# Some values might be other macros (eg FLOAT4PASSBYVAL),
		# don't change.
		elsif ($atttype eq 'bool')
		{
			$row->{$attname} = 'true' if $row->{$attname} eq 't';
			$row->{$attname} = 'false' if $row->{$attname} eq 'f';
		}

		# We don't emit initializers for the variable length fields at all.
		# Only the fixed-size portions of the descriptors are ever used.
		delete $row->{$attname} if $column->{is_varlen};
	}
}

sub usage
{
	die <<EOM;
Usage: genbki.pl [options] header...

Options:
    -o               output path
    --set-version    PostgreSQL version number for initdb cross-check

genbki.pl generates BKI files from specially formatted
header files and .dat files.  These BKI files are used
to initialize the postgres template database.

Report bugs to <pgsql-bugs\@postgresql.org>.
EOM
}
