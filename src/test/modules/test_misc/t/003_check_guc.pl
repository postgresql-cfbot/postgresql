# Tests to cross-check the consistency of GUC parameters with
# postgresql.conf.sample.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

# These are non-variables but that are mistakenly parsed as variable
# settings in the loop below.
my %skip_names =
  map { $_ => 1 } ('include', 'include_dir', 'include_if_exists');

# The following parameters have defaults which are
# environment-dependent and may not match the default
# values written in the sample config file.
my %ignore_parameters =
  map { $_ => 1 } (
	  'data_directory',
	  'hba_file',
	  'ident_file',
	  'krb_server_keyfile',
	  'timezone_abbreviations',
	  'lc_messages',
	  'max_stack_depth', # XXX
	  'wal_buffers', # XXX
	  );

# Grab the names of all the parameters that can be listed in the
# configuration sample file.  config_file is an exception, it is not
# in postgresql.conf.sample but is part of the lists from guc_tables.c.
# Custom GUCs loaded by extensions are excluded.
my $all_params = $node->safe_psql(
	'postgres',
	"SELECT name
     FROM pg_settings
   WHERE NOT 'NOT_IN_SAMPLE' = ANY (pg_settings_get_flags(name)) AND
       name <> 'config_file' AND category <> 'Customized Options'
     ORDER BY 1");
# Note the lower-case conversion, for consistency.
my @all_params_array = split("\n", lc($all_params));

# Grab the names of all parameters marked as NOT_IN_SAMPLE.
my $not_in_sample = $node->safe_psql(
	'postgres',
	"SELECT name
     FROM pg_settings
   WHERE 'NOT_IN_SAMPLE' = ANY (pg_settings_get_flags(name))
     ORDER BY 1");
my @not_in_sample_array = split("\n", lc($not_in_sample));

# use the sample file from the temp install
my $share_dir = $node->config_data('--sharedir');
my $sample_file = "$share_dir/postgresql.conf.sample";

# List of all the GUCs found in the sample file.
my @gucs_in_file;

# Read the sample file line-by-line, checking its contents to build a list
# of everything known as a GUC.
my @file_vals = ();
open(my $contents, '<', $sample_file)
  || die "Could not open $sample_file: $!";
while (my $line = <$contents>)
{
	# Check if this line matches a GUC parameter:
	# - Each parameter is preceded by "#", but not "# " in the sample
	# file.
	# - Valid configuration options are followed immediately by " = ",
	# with one space before and after the equal sign.
	if ($line =~ m/^#?([_[:alpha:]]+) = (.*)$/)
	{
		# Lower-case conversion matters for some of the GUCs.
		my $param_name = lc($1);

		# extract value
		my $file_value = $2;
		$file_value =~ s/\s*#.*$//;		# strip trailing comment
		$file_value =~ s/^'(.*)'$/$1/;	# strip quotes

		next if (defined $skip_names{$param_name});

		# Update the list of GUCs found in the sample file, for the
		# follow-up tests.
		push @gucs_in_file, $param_name;

		# Update the list of GUCs whose value is checked for consistency
		# between the sample file and pg_setting.boot_val
		if (!defined $ignore_parameters{$param_name})
		{
			push(@file_vals, [$param_name, $file_value]);
		}
	}
}

close $contents;

# Cross-check that all the GUCs found in the sample file match the ones
# fetched above.  This maps the arrays to a hash, making the creation of
# each exclude and intersection list easier.
my %gucs_in_file_hash = map { $_ => 1 } @gucs_in_file;
my %all_params_hash = map { $_ => 1 } @all_params_array;
my %not_in_sample_hash = map { $_ => 1 } @not_in_sample_array;

my @missing_from_file = grep(!$gucs_in_file_hash{$_}, @all_params_array);
is(scalar(@missing_from_file),
	0, "no parameters missing from postgresql.conf.sample");

my @missing_from_list = grep(!$all_params_hash{$_}, @gucs_in_file);
is(scalar(@missing_from_list), 0, "no parameters missing from guc_tables.c");

my @sample_intersect = grep($not_in_sample_hash{$_}, @gucs_in_file);
is(scalar(@sample_intersect),
	0, "no parameters marked as NOT_IN_SAMPLE in postgresql.conf.sample");

# These would log some information only on errors.
foreach my $param (@missing_from_file)
{
	print(
		"found GUC $param in guc_tables.c, missing from postgresql.conf.sample\n"
	);
}
foreach my $param (@missing_from_list)
{
	print(
		"found GUC $param in postgresql.conf.sample, with incorrect info in guc_tables.c\n"
	);
}
foreach my $param (@sample_intersect)
{
	print(
		"found GUC $param in postgresql.conf.sample, marked as NOT_IN_SAMPLE\n"
	);
}

# Test that GUCs in postgresql.conf.sample show the correct default values
my $check_defaults = $node->safe_psql(
	'postgres',
	"
	CREATE TABLE sample_conf AS
	SELECT m[1] AS name, COALESCE(m[3], m[5]) AS sample_value
	FROM (SELECT regexp_split_to_table(pg_read_file('$sample_file'), '\n') AS ln) conf,
	regexp_match(ln, '^#?([_[:alpha:]]+) (= ''([^'']*)''|(= ([^[:space:]]*))).*') AS m
	WHERE ln ~ '^#?[[:alpha:]]'
	");

$check_defaults = $node->safe_psql(
	'postgres',
	"
	SELECT name, sc.sample_value, boot_val
	FROM pg_settings ps
	JOIN sample_conf sc USING(name)
        JOIN pg_settings_get_flags(ps.name) AS flags ON true
	WHERE boot_val != sc.sample_value -- same value
	AND boot_val != pg_config_unitless_value(name, sc.sample_value) -- same value with different units
	AND 'DEFAULT_COMPILE' != ALL(flags) -- dynamically-set defaults
	AND 'DEFAULT_INITDB' != ALL(flags) -- dynamically-set defaults
	AND name NOT IN ('max_stack_depth', 'krb_server_keyfile'); -- exceptions
	");

my @check_defaults_array = split("\n", lc($check_defaults));

is (@check_defaults_array, 0,
	"check for consistency of defaults in postgresql.conf.sample: $check_defaults");

# XXX An alternative, perl implementation of the same thing:

# Check if GUC values in config-file and boot value match
my $values = $node->safe_psql(
	'postgres',
	'SELECT f.n, pg_config_unitless_value(f.n, f.v), s.boot_val, \'!\' '.
	'FROM (VALUES '.
	join(',', map { "('${$_}[0]','${$_}[1]')" } @file_vals).
	') f(n,v) '.
	"JOIN pg_settings s ON (s.name = f.n)".
	"JOIN pg_settings_get_flags(s.name) AS flags ON true ".
	"AND 'DEFAULT_COMPILE' != ALL(flags) -- dynamically-set defaults ".
	"AND 'DEFAULT_INITDB' != ALL(flags) -- dynamically-set defaults");

my $fails = "";
foreach my $l (split("\n", $values))
{
	# $l: <varname>|<fileval>|<boot_val>|!
	my @t = split("\\|", $l);
	if ($t[1] ne $t[2])
	{
		$fails .= "\n" if ($fails ne "");
		$fails .= "$t[0]: file \"$t[1]\" != boot_val \"$t[2]\"";
	}
}

is($fails, "", "check if GUC values in .sample and boot value match");

done_testing();
