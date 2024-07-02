# Copyright (c) 2024, PostgreSQL Global Development Group

# Tests for the log_duration_destination parameter

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);
use File::Spec::Functions;

# Start up Postgres with duration logging
my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init();
$node->append_conf(
	'postgresql.conf', qq(
logging_collector          = on
log_destination            = 'stderr, csvlog, jsonlog'
log_duration_destination   = 'stderr'
log_statement              = none
log_min_duration_statement = 0
log_rotation_age           = 0
log_rotation_size          = 0
lc_messages                = 'C'
));
$node->start();

# Send a normal duration-causing statement, then an error
$node->psql('postgres', 'SELECT 1111');
$node->psql('postgres', 'SELECT E1E1');

# Read in the current_logfiles file once it appears
my $current_logfiles;
my $attempts = 2 * $PostgreSQL::Test::Utils::timeout_default;
while ($attempts--)
{
	eval {
		$current_logfiles =
		  slurp_file(catfile($node->data_dir, 'current_logfiles'));
	};
	last unless $@;
	usleep(100_000);
}
die $@ if $@;

my $test = "File 'current_logfiles' in the data directory looks correct";
my $expected = 'stderr log/postgresql-.*\.log
csvlog log/postgresql-.*\.csv
jsonlog log/postgresql-.*\.json
duration log/postgresql-.*\.duration';
like($current_logfiles, qr{^$expected$}, $test);

# Verify the error is here the slow way, then we can check quickly
$test = 'Log file for stderr has most recent error';
check_log_pattern('stderr', $current_logfiles, 'SELECT E1E1', $node);

## Gather the text of all open log files and put into a hash
my $logtext = slurp_logfiles();

$test = 'Duration statements are written to the duration stderr log file';
like($logtext->{duration}, qr/duration.*SELECT 1111/, $test);

$test =
  'Duration statements do NOT get written to the normal stderr log file';
unlike($logtext->{stderr}, qr/SELECT 1111/, $test);

$test = 'Duration statements do NOT get written to the normal CSV log file';
unlike($logtext->{csvlog}, qr/SELECT 1111/, $test);

$test = 'Duration statements do NOT get written to the normal JSON log file';
unlike($logtext->{jsonlog}, qr/SELECT 1111/, $test);

$test = 'Error statements do NOT get written to the duration stderr log file';
unlike($logtext->{duration}, qr/SELECT E1E1/, $test);

$test = 'Errors statements are written to the normal stderr log file';
like($logtext->{stderr}, qr/SELECT E1E1/, $test);

$test = 'Errors statements are written to the normal CSV log file';
like($logtext->{csvlog}, qr/SELECT E1E1/, $test);

$test = 'Errors statements are written to the normal JSON log file';
like($logtext->{jsonlog}, qr/SELECT E1E1/, $test);

## Test CSV
$test = 'Duration CSV log file gets added to list on reload';
$node->psql('postgres', "ALTER SYSTEM SET log_duration_destination='csvlog'");
$node->psql('postgres', 'SELECT pg_reload_conf()');
$node->psql('postgres', 'SELECT 2222');
$node->psql('postgres', 'SELECT E2E2');

# Again, we do the slow way once
$current_logfiles = slurp_file($node->data_dir . '/current_logfiles');
check_log_pattern('stderr', $current_logfiles, 'SELECT E2E2', $node);

$logtext = slurp_logfiles();
ok(exists $logtext->{'duration.csv'}, $test);

$test = 'Duration stderr log file gets removed from list on reload';
ok(!exists $logtext->{'duration'}, $test);

$test = 'Duration statements are written to the duration CSV log file';
like($logtext->{'duration.csv'}, qr/SELECT 2222/, $test);

$test =
  'Duration statements do NOT get written to the normal stderr log file';
unlike($logtext->{stderr}, qr/SELECT 2222/, $test);

$test = 'Error statements do NOT get written to the duration CSV log file';
unlike($logtext->{'duration.csv'}, qr/SELECT E2E2/, $test);

## Test JSON
$test = 'Duration JSON log file gets added to list on reload';
$node->psql('postgres',
	"ALTER SYSTEM SET log_duration_destination='csvlog, jsonlog'");
$node->psql('postgres', 'SELECT pg_reload_conf()');
$node->psql('postgres', 'SELECT 3333');
$node->psql('postgres', 'SELECT E3E3');

# Wait for the error to appear
$current_logfiles = slurp_file($node->data_dir . '/current_logfiles');
check_log_pattern('stderr', $current_logfiles, 'SELECT E3E3', $node);

$logtext = slurp_logfiles();
ok(exists $logtext->{'duration.json'}, $test);

$test = 'Duration statements are written to the duration JSON log file';
like($logtext->{'duration.json'}, qr/SELECT 3333/, $test);

$test =
  'Duration statements do NOT get written to the normal stderr log file';
unlike($logtext->{stderr}, qr/SELECT 3333/, $test);

$test = 'Error statements do NOT get written to the duration JSON log file';
unlike($logtext->{'duration.json'}, qr/SELECT E3E3/, $test);

## Test all three at once
$test = 'Duration stderr log file gets added to list on reload';
$node->psql('postgres',
	"ALTER SYSTEM SET log_duration_destination='csvlog, stderr, jsonlog'");
$node->psql('postgres', 'SELECT pg_reload_conf()');
$node->psql('postgres', 'SELECT 4444');
$node->psql('postgres', 'SELECT E4E4');

# Wait for the error to appear
$current_logfiles = slurp_file($node->data_dir . '/current_logfiles');
check_log_pattern('stderr', $current_logfiles, 'SELECT E4E4', $node);

$logtext = slurp_logfiles();
ok(exists $logtext->{'duration'}, $test);

$test = 'Duration CSV log file gets added to list on reload';
ok(exists $logtext->{'duration.csv'}, $test);

$test = 'Duration JSON log file gets added to list on reload';
ok(exists $logtext->{'duration.json'}, $test);

$test = 'Duration statements are written to the duration stderr log file';
like($logtext->{'duration'}, qr/SELECT 4444/, $test);

$test = 'Duration statements are written to the duration CSV log file';
like($logtext->{'duration.csv'}, qr/SELECT 4444/, $test);

$test = 'Duration statements are written to the duration JSON log file';
like($logtext->{'duration.json'}, qr/SELECT 4444/, $test);

$test =
  'Duration statements do NOT get written to the normal stderr log file';
unlike($logtext->{stderr}, qr/SELECT 4444/, $test);

$test = 'Error statements do NOT get written to the duration stderr log file';
unlike($logtext->{'duration.json'}, qr/SELECT E4E4/, $test);

$test = 'Error statements do NOT get written to the duration CSV log file';
unlike($logtext->{'duration.csv'}, qr/SELECT E4E4/, $test);

$test = 'Error statements do NOT get written to the duration JSON log file';
unlike($logtext->{'duration.json'}, qr/SELECT E4E4/, $test);

## Test log rotation
my %oldname;
while ($current_logfiles =~ /^(\S+) (.+?)$/gsm)
{
	$oldname{$1} = $2;
}
sleep 2;
$node->psql('postgres', "SELECT pg_rotate_logfile()");
$current_logfiles = slurp_file($node->data_dir . '/current_logfiles');

my %newname;
while ($current_logfiles =~ /^(\S+) (.+?)$/gsm)
{
	$newname{$1} = $2;
}

$test = "Log file has rotated to a new name";
for my $type (sort keys %oldname)
{
	my $old = $oldname{$type};
	my $new = $newname{$type};
	isnt($old, $new, "$test (type=$type)");
}

## Test no duration files at all
$test = 'Duration stderr gets removed from list on reload';
$node->psql('postgres', "ALTER SYSTEM SET log_duration_destination=''");
$node->psql('postgres', 'SELECT pg_reload_conf()');
$node->psql('postgres', 'SELECT 5555');
$node->psql('postgres', 'SELECT E5E5');

# Wait for the error to appear
$current_logfiles = slurp_file($node->data_dir . '/current_logfiles');
check_log_pattern('stderr', $current_logfiles, 'SELECT E5E5', $node);

$logtext = slurp_logfiles();
ok(!exists $logtext->{'duration'}, $test);

$test = 'Duration CSV gets removed from list on reload';
ok(!exists $logtext->{'duration.csv'}, $test);

$test = 'Duration JSON gets removed from list on reload';
ok(!exists $logtext->{'duration.json'}, $test);

$test = 'Duration statements are written to the normal stderr log file';
like($logtext->{'stderr'}, qr/SELECT 5555/, $test);


done_testing();

exit;


sub slurp_logfiles
{

	# Reload the current_logfiles file from the data_directory
	$current_logfiles = slurp_file($node->data_dir . '/current_logfiles');

	# Grab each one, and extract its contents into the hash
	my %logtext;
	while ($current_logfiles =~ /^(\S+) (.+?)$/gsm)
	{
		my ($type, $name) = ($1, $2);
		my $path = catfile($node->data_dir, $name);
		$logtext{$type} = slurp_file($path);
	}
	return \%logtext;
}

# Check for a pattern in the logs associated to one format.
sub check_log_pattern
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my $format = shift;
	my $logfiles = shift;
	my $pattern = shift;
	my $node = shift;
	my $lfname = fetch_file_name($logfiles, $format);

	my $max_attempts = 10 * $PostgreSQL::Test::Utils::timeout_default;

	my $logcontents;
	for (my $attempts = 0; $attempts < $max_attempts; $attempts++)
	{
		$logcontents = slurp_file($node->data_dir . '/' . $lfname);
		last if $logcontents =~ m/$pattern/;
		usleep(100_000);
	}

	like($logcontents, qr/$pattern/,
		"found expected log file content for $format");

	return;
}

# Extract the file name of a $format from the contents of current_logfiles.
sub fetch_file_name
{
	my $logfiles = shift;
	my $format = shift;
	my @lines = split(/\n/, $logfiles);
	my $filename = undef;
	foreach my $line (@lines)
	{
		if ($line =~ /$format (.*)$/gm)
		{
			$filename = $1;
		}
	}

	return $filename;
}
