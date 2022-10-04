
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Set of tests for authentication and pg_hba.conf inclusion.
# This test can only run with Unix-domain sockets.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);
use IPC::Run qw(pump finish timer);
use Data::Dumper;

if (!$use_unix_sockets)
{
	plan skip_all =>
	  "authentication tests cannot run without Unix-domain sockets";
}

# stores the current line counter for each file.  hba_rule and ident_rule are
# fake file names used for the global rule number for each auth view.
my %cur_line = ('hba_rule' => 1, 'ident_rule' => 1);

my $hba_file = 'subdir1/pg_hba_custom.conf';
my $ident_file = 'subdir2/pg_ident_custom.conf';

# Initialize primary node
my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->start;

my $data_dir = $node->data_dir;

# Normalize the data directory for Windows
$data_dir =~ s/\/\.\//\//g; # reduce /./ to /
$data_dir =~ s/\/\//\//g;   # reduce // to /
$data_dir =~ s/\/$//;       # remove trailing /


# Add the given payload to the given relative HBA file of the given node.
# This function maintains the %cur_line metadata, so it has to be called in the
# expected inclusion evaluation order in order to keep it in sync.
#
# If the payload starts with "include" or "ignore", the function doesn't
# increase the general hba rule number.
#
# If an err_str is provided, it returns an arrayref containing the provided
# filename, the current line number in that file and the provided err_str.  The
# err_str has to be a valid regex string.
# Otherwise it only returns the line number of the payload in the wanted file.
# This function has to be called in the expected inclusion evaluation order to
# keep the %cur_line information in sync.
sub add_hba_line
{
	my $node     = shift;
	my $filename = shift;
	my $payload  = shift;
	my $err_str  = shift;
	my $globline;
	my $fileline;
	my @tokens;
	my $line;

	# Append the payload to the given file
	$node->append_conf($filename, $payload);

	# Get the current %cur_line counter for the file
	if (not defined $cur_line{$filename})
	{
		$cur_line{$filename} = 1;
	}
	$fileline = $cur_line{$filename}++;

	# Include directive, don't generate an underlying pg_hba_file_rules line
	# but make sure we incremented the %cur_line counter.
	# Also ignore line beginning with "ignore", for content of files that
	# should not being included
	if ($payload =~ qr/^(include|ignore)/)
	{
		if (defined $err_str)
		{
			return [$filename, $fileline, $err_str];
		}
		else
		{
			return $fileline;
		}
	}

	# Get (and increment) the global rule number
	$globline = $cur_line{'hba_rule'}++;

	# If caller provided an err_str, just returns the needed metadata
	if (defined $err_str)
	{
		return [$filename, $fileline, $err_str];
	}

	# Otherwise, generate the expected pg_hba_file_rules line
	@tokens = split(/ /, $payload);
	$tokens[1] = '{' . $tokens[1] . '}'; # database
	$tokens[2] = '{' . $tokens[2] . '}'; # user_name

	# add empty address and netmask betweed user_name and auth_method
	splice @tokens, 3, 0, '';
	splice @tokens, 3, 0, '';

	# append empty options and error
	push @tokens, '';
	push @tokens, '';

	# generate the expected final line
	$line = "";
	$line .= "\n" if ($globline > 1);
	$line .= "$globline|$data_dir/$filename|$fileline|";
	$line .= join('|', @tokens);

	return $line;
}

# Add the given payload to the given relative ident file of the given node.
# Same as add_hba_line but for pg_ident files
sub add_ident_line
{
	my $node     = shift;
	my $filename = shift;
	my $payload  = shift;
	my $err_str  = shift;
	my $globline;
	my $fileline;
	my @tokens;
	my $line;

	# Append the payload to the given file
	$node->append_conf($filename, $payload);

	# Get the current %cur_line counter for the file
	if (not defined $cur_line{$filename})
	{
		$cur_line{$filename} = 1;
	}
	$fileline = $cur_line{$filename}++;

	# Include directive, don't generate an underlying pg_hba_file_rules line
	# but make sure we incremented the %cur_line counter.
	# Also ignore line beginning with "ignore", for content of files that
	# should not being included
	if ($payload =~ qr/^(include|ignore)/)
	{
		if (defined $err_str)
		{
			return [$filename, $fileline, $err_str];
		}
		else
		{
			return $fileline;
		}
	}

	# Get (and increment) the global rule number
	$globline = $cur_line{'ident_rule'}++;

	# If caller provided an err_str, just returns the needed metadata
	if (defined $err_str)
	{
		return [$filename, $fileline, $err_str];
	}

	# Otherwise, generate the expected pg_ident_file_mappings line
	@tokens = split(/ /, $payload);

	# append empty error
	push @tokens, '';

	# generate the expected final line
	$line = "";
	$line .= "\n" if ($globline > 1);
	$line .= "$globline|$data_dir/$filename|$fileline|";
	$line .= join('|', @tokens);

	return $line;
}

# Delete pg_hba.conf from the given node, add various entries to test the
# include infrastructure and then execute a reload to refresh it.
sub generate_valid_auth_files
{
	my $node       = shift;
	my $hba_expected = '';
	my $ident_expected = '';

	# customise main auth file names
	$node->safe_psql('postgres', "ALTER SYSTEM SET hba_file = '$data_dir/$hba_file'");
	$node->safe_psql('postgres', "ALTER SYSTEM SET ident_file = '$data_dir/$ident_file'");

	# and make original ones invalid to be sure they're not used anywhere
	$node->append_conf('pg_hba.conf', "some invalid line");
	$node->append_conf('pg_ident.conf', "some invalid line");

	# pg_hba stuff
	mkdir("$data_dir/subdir1");
	mkdir("$data_dir/hba_inc");
	mkdir("$data_dir/hba_inc_if");
	mkdir("$data_dir/hba_pos");

	# Make sure we will still be able to connect
	$hba_expected .= add_hba_line($node, "$hba_file", 'local all all trust');

	# Add include data
	add_hba_line($node, "$hba_file", "include ../pg_hba_pre.conf");
	$hba_expected .= add_hba_line($node, 'pg_hba_pre.conf', "local pre all reject");

	$hba_expected .= add_hba_line($node, "$hba_file", "local all all reject");

	add_hba_line($node, "$hba_file", "include ../hba_pos/pg_hba_pos.conf");
	$hba_expected .= add_hba_line($node, 'hba_pos/pg_hba_pos.conf', "local pos all reject");
	# include is relative to current path
	add_hba_line($node, 'hba_pos/pg_hba_pos.conf', "include pg_hba_pos2.conf");
	$hba_expected .= add_hba_line($node, 'hba_pos/pg_hba_pos2.conf', "local pos2 all reject");

	# include_if_exists data
	add_hba_line($node, "$hba_file", "include_if_exists ../hba_inc_if/none");
	add_hba_line($node, "$hba_file", "include_if_exists ../hba_inc_if/some");
	$hba_expected .= add_hba_line($node, 'hba_inc_if/some', "local if_some all reject");

	# include_dir data
	add_hba_line($node, "$hba_file", "include_dir ../hba_inc");
	add_hba_line($node, 'hba_inc/garbageconf', "ignore - should not be included");
	$hba_expected .= add_hba_line($node, 'hba_inc/01_z.conf', "local dir_z all reject");
	$hba_expected .= add_hba_line($node, 'hba_inc/02_a.conf', "local dir_a all reject");

	# secondary auth file
	add_hba_line($node, $hba_file, 'local @../dbnames.conf all reject');
	$node->append_conf('dbnames.conf', "db1");
	$node->append_conf('dbnames.conf', "db3");
	$hba_expected .= "\n" . ($cur_line{'hba_rule'} - 1)
		. "|$data_dir/$hba_file|" . ($cur_line{$hba_file} - 1)
		. '|local|{db1,db3}|{all}|||reject||';

	# pg_ident stuff
	mkdir("$data_dir/subdir2");
	mkdir("$data_dir/ident_inc");
	mkdir("$data_dir/ident_inc_if");
	mkdir("$data_dir/ident_pos");

	# Add include data
	add_ident_line($node, "$ident_file", "include ../pg_ident_pre.conf");
	$ident_expected .= add_ident_line($node, 'pg_ident_pre.conf', "pre foo bar");

	$ident_expected .= add_ident_line($node, "$ident_file", "test a b");

	add_ident_line($node, "$ident_file", "include ../ident_pos/pg_ident_pos.conf");
	$ident_expected .= add_ident_line($node, 'ident_pos/pg_ident_pos.conf', "pos foo bar");
	# include is relative to current path
	add_ident_line($node, 'ident_pos/pg_ident_pos.conf', "include pg_ident_pos2.conf");
	$ident_expected .= add_ident_line($node, 'ident_pos/pg_ident_pos2.conf', "pos2 foo bar");

	# include_if_exists data
	add_ident_line($node, "$ident_file", "include_if_exists ../ident_inc_if/none");
	add_ident_line($node, "$ident_file", "include_if_exists ../ident_inc_if/some");
	$ident_expected .= add_ident_line($node, 'ident_inc_if/some', "if_some foo bar");

	# include_dir data
	add_ident_line($node, "$ident_file", "include_dir ../ident_inc");
	add_ident_line($node, 'ident_inc/garbageconf', "ignore - should not be included");
	$ident_expected .= add_ident_line($node, 'ident_inc/01_z.conf', "dir_z foo bar");
	$ident_expected .= add_ident_line($node, 'ident_inc/02_a.conf', "dir_a foo bar");

	$node->restart;
	$node->connect_ok('dbname=postgres',
		'Connection ok after generating valid auth files');

	return ($hba_expected, $ident_expected);
}

# Delete pg_hba.conf and pg_ident.conf from the given node and add minimal
# entries to allow authentication.
sub reset_auth_files
{
	my $node       = shift;

	unlink("$data_dir/$hba_file");
	unlink("$data_dir/$ident_file");

	%cur_line = ('hba_rule' => 1, 'ident_rule' => 1);

	return add_hba_line($node, "$hba_file", 'local all all trust');
}

# Generate a list of expected error regex for the given array of error
# conditions, as generated by add_hba_line/add_ident_line with an err_str.
#
# 2 regex are generated per array entry: one for the given err_str, and one for
# the expected line in the specific file.  Since all lines are independant,
# there's no guarantee that a specific failure regex and the per-line regex
# will match the same error.  Calling code should add at least one test with a
# single error to make sure that the line number / file name is correct.
#
# On top of that, an extra line is generated for the general failure to process
# the main auth file.
sub generate_log_err_patterns
{
	my $node       = shift;
	my $raw_errors = shift;
	my $is_hba_err = shift;
	my @errors;

	foreach my $arr (@{$raw_errors})
	{
		my $filename = @{$arr}[0];
		my $fileline = @{$arr}[1];
		my $err_str = @{$arr}[2];

		push @errors, qr/$err_str/;

		# Context messages with the file / line location aren't always emitted
		if ($err_str !~ /maximum nesting depth exceeded/ and
			$err_str !~ /could not open secondary authentication file/)
		{
			push @errors, qr/line $fileline of configuration file "$data_dir\/$filename"/
		}
	}

	push @errors, qr/could not load $data_dir\/$hba_file/ if ($is_hba_err);

	return \@errors;
}

# Generate the expected output for the auth file view error reporting (file
# name, file line, error), for the given array of error conditions, as
# generated generated by add_hba_line/add_ident_line with an err_str.
sub generate_log_err_rows
{
	my $node       = shift;
	my $raw_errors = shift;
	my $exp_rows   = '';

	foreach my $arr (@{$raw_errors})
	{
		my $filename = @{$arr}[0];
		my $fileline = @{$arr}[1];
		my $err_str = @{$arr}[2];

		$exp_rows .= "\n" if ($exp_rows ne "");

		# Unescape regex patterns if any
		$err_str =~ s/\\([\(\)])/$1/g;
		$exp_rows .= "|$data_dir\/$filename|$fileline|$err_str"
	}

	return $exp_rows;
}

# Reset the main auth files, append the given payload to the given config file,
# and check that the instance cannot start, raising the expected error line(s).
sub start_errors_like
{
	my $node        = shift;
	my $file        = shift;
	my $payload     = shift;
	my $pattern     = shift;
	my $should_fail = shift;

	reset_auth_files($node);
	$node->append_conf($file, $payload);

	unlink($node->logfile);
	my $ret =
		PostgreSQL::Test::Utils::system_log('pg_ctl', '-D', $data_dir,
		'-l', $node->logfile, 'start');

	if ($should_fail)
	{
		ok($ret != 0, "Cannot start postgres with faulty $file");
	}
	else
	{
		ok($ret == 0, "postgres can start with faulty $file");
	}

	my $log_contents = slurp_file($node->logfile);

	foreach (@{$pattern})
	{
		like($log_contents,
			$_,
			"Expected failure found in the logs");
	}

	if (not $should_fail)
	{
		# We can't simply call $node->stop here as the call is optimized out
		# when the server isn't started with $node->start.
		my $ret =
			PostgreSQL::Test::Utils::system_log('pg_ctl', '-D',
			$data_dir, 'stop', '-m', 'fast');
		ok($ret == 0, "Could stop postgres");
	}
}

# We should be able to connect, and see an empty pg_ident.conf
is($node->psql(
		'postgres', 'SELECT count(*) FROM pg_ident_file_mappings'),
	qq(0),
	'pg_ident.conf is empty');

############################################
# part 1, test view reporting for valid data
############################################
my ($exp_hba, $exp_ident) = generate_valid_auth_files($node);

$node->connect_ok('dbname=postgres', 'Connection still ok');

is($node->safe_psql(
		'postgres', 'SELECT * FROM pg_hba_file_rules'),
	qq($exp_hba),
	'pg_hba_file_rules content is expected');

is($node->safe_psql(
		'postgres', 'SELECT * FROM pg_ident_file_mappings'),
	qq($exp_ident),
	'pg_ident_file_mappings content is expected');

#############################################
# part 2, test log reporting for invalid data
#############################################
reset_auth_files($node);
$node->restart('fast');
$node->connect_ok('dbname=postgres',
	'Connection ok after resetting auth files');

$node->stop('fast');

start_errors_like($node, $hba_file, "include ../not_a_file",
	[
		qr/could not open included authentication file "\.\.\/not_a_file" as "$data_dir\/not_a_file": No such file or directory/,
		qr/could not load $data_dir\/$hba_file/
	], 1);

# include_dir, single included file
mkdir("$data_dir/hba_inc_fail");
add_hba_line($node, "hba_inc_fail/inc_dir.conf", "local all all reject");
add_hba_line($node, "hba_inc_fail/inc_dir.conf", "local all all reject");
add_hba_line($node, "hba_inc_fail/inc_dir.conf", "local all all reject");
add_hba_line($node, "hba_inc_fail/inc_dir.conf", "not_a_token");
start_errors_like($node, $hba_file, "include_dir ../hba_inc_fail",
	[
		qr/invalid connection type "not_a_token"/,
		qr/line 4 of configuration file "$data_dir\/hba_inc_fail\/inc_dir\.conf"/,
		qr/could not load $data_dir\/$hba_file/
	], 1);

# include_dir, single included file with nested inclusion
unlink("$data_dir/hba_inc_fail/inc_dir.conf");
my @hba_raw_errors_step1;

add_hba_line($node, "hba_inc_fail/inc_dir.conf", "include file1");

add_hba_line($node, "hba_inc_fail/file1", "include file2");
add_hba_line($node, "hba_inc_fail/file2", "local all all reject");
add_hba_line($node, "hba_inc_fail/file2", "include file3");

add_hba_line($node, "hba_inc_fail/file3", "local all all reject");
add_hba_line($node, "hba_inc_fail/file3", "local all all reject");
push @hba_raw_errors_step1, add_hba_line($node, "hba_inc_fail/file3",
	"local all all zuul",
	'invalid authentication method "zuul"');

start_errors_like(
	$node, $hba_file, "include_dir ../hba_inc_fail",
	generate_log_err_patterns($node, \@hba_raw_errors_step1, 1), 1);

# start_errors_like will reset the main auth files, so the previous error won't
# occur again.  We keep it around as we will put back both bogus inclusions for
# the tests at step 3.
my @hba_raw_errors_step2;

# include_if_exists, with various problems
push @hba_raw_errors_step2, add_hba_line($node, "hba_if_exists.conf",
	"local",
	"end-of-line before database specification");
push @hba_raw_errors_step2, add_hba_line($node, "hba_if_exists.conf",
	"local,host",
	"multiple values specified for connection type");
push @hba_raw_errors_step2, add_hba_line($node, "hba_if_exists.conf",
	"local all",
	"end-of-line before role specification");
push @hba_raw_errors_step2, add_hba_line($node, "hba_if_exists.conf",
	"local all all",
	"end-of-line before authentication method");
push @hba_raw_errors_step2, add_hba_line($node, "hba_if_exists.conf",
	"host all all test/42",
	'specifying both host name and CIDR mask is invalid: "test/42"');
push @hba_raw_errors_step2, add_hba_line($node, "hba_if_exists.conf",
	'local @dbnames_fails.conf all reject',
	"could not open secondary authentication file \"\@dbnames_fails.conf\" as \"$data_dir/dbnames_fails.conf\": No such file or directory");

add_hba_line($node, "hba_if_exists.conf", "include recurse.conf");
push @hba_raw_errors_step2, add_hba_line($node, "recurse.conf",
	"include recurse.conf",
	'could not open configuration file "recurse.conf": maximum nesting depth exceeded');

# Generate the regex for the expected errors in the logs.  There's no guarantee
# that the generated "line X of file..." will be emitted for the expected line,
# but previous tests already ensured that the correct line number / file name
# was emitted, so ensuring that there's an error in all expected lines is
# enough here.
my $expected_errors = generate_log_err_patterns($node, \@hba_raw_errors_step2,
	1);

# Not an error, but it should raise a message in the logs.  Manually add an
# extra log message to detect
add_hba_line($node, "hba_if_exists.conf", "include_if_exists if_exists_none");
push @{$expected_errors},
	qr/skipping missing authentication file "$data_dir\/if_exists_none"/;

start_errors_like(
	$node, $hba_file, "include_if_exists ../hba_if_exists.conf",
	$expected_errors, 1);

# Mostly the same, but for ident files
reset_auth_files($node);

my @ident_raw_errors_step1;

# include_dir, single included file with nested inclusion
mkdir("$data_dir/ident_inc_fail");
add_ident_line($node, "ident_inc_fail/inc_dir.conf", "include file1");

add_ident_line($node, "ident_inc_fail/file1", "include file2");
add_ident_line($node, "ident_inc_fail/file2", "ok ok ok");
add_ident_line($node, "ident_inc_fail/file2", "include file3");

add_ident_line($node, "ident_inc_fail/file3", "ok ok ok");
add_ident_line($node, "ident_inc_fail/file3", "ok ok ok");
push @ident_raw_errors_step1, add_ident_line($node, "ident_inc_fail/file3",
	"failmap /(fail postgres",
	'invalid regular expression "\(fail": parentheses \(\) not balanced');

start_errors_like(
	$node, $ident_file, "include_dir ../ident_inc_fail",
	generate_log_err_patterns($node, \@ident_raw_errors_step1, 0),
	0);

# start_errors_like will reset the main auth files, so the previous error won't
# occur again.  We keep it around as we will put back both bogus inclusions for
# the tests at step 3.
my @ident_raw_errors_step2;

# include_if_exists, with various problems
push @ident_raw_errors_step2, add_ident_line($node, "ident_if_exists.conf", "map",
	"missing entry at end of line");
push @ident_raw_errors_step2, add_ident_line($node, "ident_if_exists.conf", "map1,map2",
	"multiple values in ident field");
push @ident_raw_errors_step2, add_ident_line($node, "ident_if_exists.conf",
	'map @osnames_fails.conf postgres',
	"could not open secondary authentication file \"\@osnames_fails.conf\" as \"$data_dir/osnames_fails.conf\": No such file or directory");

add_ident_line($node, "ident_if_exists.conf", "include ident_recurse.conf");
push @ident_raw_errors_step2, add_ident_line($node, "ident_recurse.conf", "include ident_recurse.conf",
	'could not open configuration file "ident_recurse.conf": maximum nesting depth exceeded');

start_errors_like(
	$node, $ident_file, "include_if_exists ../ident_if_exists.conf",
	# There's no guarantee that the generated "line X of file..." will be
	# emitted for the expected line, but previous tests already ensured that
	# the correct line number / file name was emitted, so ensuring that there's
	# an error in all expected lines is enough here.
	generate_log_err_patterns($node, \@ident_raw_errors_step2, 0),
	0);

#####################################################
# part 3, test reporting of various error scenario
# NOTE: this will be bypassed -DEXEC_BACKEND or win32
#####################################################
reset_auth_files($node);

$node->start;
$node->connect_ok('dbname=postgres', 'Can connect after an auth file reset');

is($node->safe_psql(
	'postgres',
	'SELECT count(*) FROM pg_hba_file_rules WHERE error IS NOT NULL'),
	qq(0),
	'No error expected in pg_hba_file_rules');

add_ident_line($node, $ident_file, '');
is($node->safe_psql(
	'postgres',
	'SELECT count(*) FROM pg_ident_file_mappings WHERE error IS NOT NULL'),
	qq(0),
	'No error expected in pg_ident_file_mappings');

# The instance could be restarted and no error is detected.  Now check if the
# build is compatible with the view error reporting (EXEC_BACKEND / win32 will
# fail when trying to connect as they always rely on the current auth files
# content)
my @hba_raw_errors;

push @hba_raw_errors, add_hba_line($node, $hba_file, "include ../not_a_file",
	"could not open included authentication file \"../not_a_file\" as \"$data_dir/not_a_file\": No such file or directory");

my ($stdout, $stderr);
my $cmdret = $node->psql('postgres', 'SELECT 1',
	stdout => \$stdout, stderr => \$stderr);

if ($cmdret != 0)
{
	# Connection failed.  Bail out, but make sure to raise a failure if it
	# didn't fail for the expected hba file modification.
	like($stderr,
		qr/connection to server.* failed: FATAL:  could not load $data_dir\/$hba_file/,
		"Connection failed due to loading an invalid hba file");

	done_testing();
	diag("Build not compatible with auth file view error reporting, bail out.\n");
	exit;
}

# Combine errors generated at step 2, in the same order.
$node->append_conf($hba_file, "include_dir ../hba_inc_fail");
push @hba_raw_errors, @hba_raw_errors_step1;

$node->append_conf($hba_file, "include_if_exists ../hba_if_exists.conf");
push @hba_raw_errors, @hba_raw_errors_step2;

my $hba_expected = generate_log_err_rows($node, \@hba_raw_errors);
is($node->safe_psql(
	'postgres',
	'SELECT rule_number, file_name, line_number, error FROM pg_hba_file_rules'
	. ' WHERE error IS NOT NULL ORDER BY rule_number'),
	qq($hba_expected),
	'Detected all error in hba file');

# and do the same for pg_ident
my @ident_raw_errors;

push @ident_raw_errors, add_ident_line($node, $ident_file, "include ../not_a_file",
	"could not open included authentication file \"../not_a_file\" as \"$data_dir/not_a_file\": No such file or directory");

$node->append_conf($ident_file, "include_dir ../ident_inc_fail");
push @ident_raw_errors, @ident_raw_errors_step1;

$node->append_conf($ident_file, "include_if_exists ../ident_if_exists.conf");
push @ident_raw_errors, @ident_raw_errors_step2;

my $ident_expected = generate_log_err_rows($node, \@ident_raw_errors);
is($node->safe_psql(
	'postgres',
	'SELECT mapping_number, file_name, line_number, error FROM pg_ident_file_mappings'
	. ' WHERE error IS NOT NULL ORDER BY mapping_number'),
	qq($ident_expected),
	'Detected all error in ident file');

done_testing();
