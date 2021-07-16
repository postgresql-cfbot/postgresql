use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 2;
use Time::HiRes qw(usleep);

# Set up node with logging collector
my $node = get_new_node('primary');
$node->init();
$node->append_conf(
	'postgresql.conf', qq(
logging_collector = on
lc_messages = 'C'
));

$node->start();

# Verify that log output gets to the file
$node->psql('postgres', 'select pg_print_backtrace(pg_backend_pid())');

# might need to retry if logging collector process is slow...
my $max_attempts = 180 * 10;

my $current_logfiles;
for (my $attempts = 0; $attempts < $max_attempts; $attempts++)
{
	eval {
		$current_logfiles = slurp_file($node->data_dir . '/current_logfiles');
	};
	last unless $@;
	usleep(100_000);
}
die $@ if $@;

note "current_logfiles = $current_logfiles";

like(
	$current_logfiles,
	qr|^stderr log/postgresql-.*log$|,
	'current_logfiles is sane');

my $lfname = $current_logfiles;
$lfname =~ s/^stderr //;
chomp $lfname;

my $first_logfile;
my $bt_occurence_count;

# Verify that the backtraces of the processes are logged into logfile.
for (my $attempts = 0; $attempts < $max_attempts; $attempts++)
{
	$first_logfile = $node->data_dir . '/' . $lfname;
	chomp $first_logfile;
	print "file is $first_logfile";
	open my $fh, '<', $first_logfile
	  or die "Could not open '$first_logfile' $!";
	while (my $line = <$fh>)
	{
		chomp $line;
		if ($line =~ m/current backtrace/)
		{
			$bt_occurence_count++;
		}
	}
	last if $bt_occurence_count == 1;
	usleep(100_000);
}

is($bt_occurence_count, 1, 'found expected backtrace in the log file');

$node->stop();
