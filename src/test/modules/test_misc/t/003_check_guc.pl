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

# Grab the names of all the parameters that can be listed in the
# configuration sample file.  config_file is an exception, it is not
# in postgresql.conf.sample but is part of the lists from guc.c.
my $all_params = $node->safe_psql(
	'postgres',
	"SELECT name
     FROM pg_settings
   WHERE NOT 'NOT_IN_SAMPLE' = ANY (pg_settings_get_flags(name)) AND
       name <> 'config_file'
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

# TAP tests are executed in the directory of the test, in the source tree,
# even for VPATH builds, so rely on that to find postgresql.conf.sample.
my $rootdir     = "../../../..";
my $sample_file = "$rootdir/src/backend/utils/misc/postgresql.conf.sample";

# List of all the GUCs found in the sample file.
my @gucs_in_file;

# Read the sample file line-by-line, checking its contents to build a list
# of everything known as a GUC.
my $num_tests = 0;
open(my $contents, '<', $sample_file)
  || die "Could not open $sample_file: $!";
while (my $line = <$contents>)
{
	# Check if this line matches a GUC parameter:
	# - Each parameter is preceded by "#", but not "# " in the sample
	# file.
	# - Valid configuration options are followed immediately by " = ",
	# with one space before and after the equal sign.
	if ($line =~ m/^#?([_[:alpha:]]+) = .*/)
	{
		# Lower-case conversion matters for some of the GUCs.
		my $param_name = lc($1);

		# Ignore some exceptions.
		next if $param_name eq "include";
		next if $param_name eq "include_dir";
		next if $param_name eq "include_if_exists";

		# Update the list of GUCs found in the sample file, for the
		# follow-up tests.
		push @gucs_in_file, $param_name;
	}
}

close $contents;

# Cross-check that all the GUCs found in the sample file match the ones
# fetched above.  This maps the arrays to a hash, making the creation of
# each exclude and intersection list easier.
my %gucs_in_file_hash  = map { $_ => 1 } @gucs_in_file;
my %all_params_hash    = map { $_ => 1 } @all_params_array;
my %not_in_sample_hash = map { $_ => 1 } @not_in_sample_array;

my @missing_from_file = grep(!$gucs_in_file_hash{$_}, @all_params_array);
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

# Test that GUCs in postgresql.conf.sample show the correct default values
my $check_defaults = $node->safe_psql(
	'postgres',
	"
	CREATE TABLE sample_conf AS
	SELECT m[1] AS name, COALESCE(m[3], m[5]) AS sample_value
	FROM (SELECT regexp_split_to_table(pg_read_file('../../../$sample_file'), '\n') AS ln) conf,
	regexp_match(ln, '^#?([_[:alpha:]]+) (= ''([^'']*)''|(= ([^[:space:]]*))).*') AS m
	WHERE ln ~ '^#?[[:alpha:]]'
	");

$check_defaults = $node->safe_psql(
	'postgres',
	"
	SELECT name, current_setting(name), sc.sample_value, boot_val
	FROM pg_show_all_settings() psas
	JOIN sample_conf sc USING(name)
	WHERE sc.sample_value != boot_val -- same value
	AND sc.sample_value != current_setting(name) -- same value, with human units
	AND sc.sample_value != current_setting(name)||'.0' -- same value with .0 suffix
	AND 'DYNAMIC_DEFAULT' != ALL(pg_settings_get_flags(psas.name)) -- dynamically-set defaults
	AND NOT (sc.sample_value ~ '^0' AND current_setting(name) ~ '^0') -- zeros may be written differently
	AND NOT (sc.sample_value='60s' AND current_setting(name) = '1min') -- two ways to write 1min
	AND name NOT IN ('krb_server_keyfile', 'wal_retrieve_retry_interval', 'log_autovacuum_min_duration'); -- exceptions
	");

my @check_defaults_array = split("\n", lc($check_defaults));
print(STDERR "$check_defaults\n");

is (@check_defaults_array, 0,
	'check for consistency of defaults in postgresql.conf.sample');

done_testing();
