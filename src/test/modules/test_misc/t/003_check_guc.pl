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

# parameter names that cannot get consistency check performed
my @ignored_parameters = (
	'data_directory',
	'hba_file',
	'ident_file',
	'krb_server_keyfile',
	'max_stack_depth',
	'bgwriter_flush_after',
	'wal_sync_method',
	'checkpoint_flush_after',
	'timezone_abbreviations',
	'lc_messages'
  );

# Grab the names of all the parameters that can be listed in the
# configuration sample file.  config_file is an exception, it is not
# in postgresql.conf.sample but is part of the lists from guc.c.
my $all_params = $node->safe_psql(
	'postgres',
	"SELECT lower(name), vartype, unit, boot_val, '!'
     FROM pg_settings
   WHERE NOT 'NOT_IN_SAMPLE' = ANY (pg_settings_get_flags(name)) AND
       name <> 'config_file'
     ORDER BY 1");
# Note the lower-case conversion, for consistency.
my %all_params_hash;
foreach my $line (split("\n", $all_params))
{
	my @f = split('\|', $line);
	fail("query returned wrong number of columns: $#f : $line") if ($#f != 4);
	$all_params_hash{$f[0]}->{type} = $f[1];
	$all_params_hash{$f[0]}->{unit} = $f[2];
	$all_params_hash{$f[0]}->{bootval} = $f[3];
}

# Grab the names of all parameters marked as NOT_IN_SAMPLE.
my $not_in_sample = $node->safe_psql(
	'postgres',
	"SELECT name
     FROM pg_settings
   WHERE 'NOT_IN_SAMPLE' = ANY (pg_settings_get_flags(name))
     ORDER BY 1");
my @not_in_sample_array = split("\n", lc($not_in_sample));

# TAP tests are executed in the directory of the test, in the source tree,
# even for VPATH builds, so rely on that to find postgresql.conf.sample.
my $rootdir     = "../../../..";
my $sample_file = "$rootdir/src/backend/utils/misc/postgresql.conf.sample";

# List of all the GUCs found in the sample file.
my @gucs_in_file;

# Read the sample file line-by-line, checking its contents to build a list
# of everything known as a GUC.
my @check_elems = ();
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

		# Ignore some exceptions.
		next if $param_name eq "include";
		next if $param_name eq "include_dir";
		next if $param_name eq "include_if_exists";

		# Update the list of GUCs found in the sample file, for the
		# follow-up tests.
		push @gucs_in_file, $param_name;

		# Check for consistency between bootval and file value.
		if (!grep { $_ eq $param_name } @ignored_parameters)
		{
			push (@check_elems, "('$param_name','$file_value')");
		}
	}
}

close $contents;

# run consistency check between config-file's default value and boot values.
my $check_query =
  'SELECT f.n, f.v, s.boot_val FROM (VALUES '.
  join(',', @check_elems).
  ') f(n,v) JOIN pg_settings s ON s.name = f.n '.
  'JOIN pg_settings_get_flags(s.name) flags ON true '.
  "WHERE 'DYNAMIC_DEFAULT' <> ALL(flags) AND " .
  'pg_normalize_config_value(f.n, f.v) <> '.
  'pg_normalize_config_value(f.n, s.boot_val)';

is ($node->safe_psql('postgres', $check_query), '',
	'check if fileval-bootval consistency is fine');

# Cross-check that all the GUCs found in the sample file match the ones
# fetched above.  This maps the arrays to a hash, making the creation of
# each exclude and intersection list easier.
my %gucs_in_file_hash  = map { $_ => 1 } @gucs_in_file;
my %not_in_sample_hash = map { $_ => 1 } @not_in_sample_array;

my @missing_from_file = grep(!$gucs_in_file_hash{$_}, keys %all_params_hash);
is(scalar(@missing_from_file),
	0, "no parameters missing from postgresql.conf.sample");

my @missing_from_list = grep(!$all_params_hash{$_}, @gucs_in_file);
is(scalar(@missing_from_list), 0, "no parameters missing from guc.c");

my @sample_intersect = grep($not_in_sample_hash{$_}, @gucs_in_file);
is(scalar(@sample_intersect),
	0, "no parameters marked as NOT_IN_SAMPLE in postgresql.conf.sample");

# These would log some information only on errors.
foreach my $param (@missing_from_file)
{
	print("found GUC $param in guc.c, missing from postgresql.conf.sample\n");
}
foreach my $param (@missing_from_list)
{
	print(
		"found GUC $param in postgresql.conf.sample, with incorrect info in guc.c\n"
	);
}
foreach my $param (@sample_intersect)
{
	print(
		"found GUC $param in postgresql.conf.sample, marked as NOT_IN_SAMPLE\n"
	);
}

done_testing();
