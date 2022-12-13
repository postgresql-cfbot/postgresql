
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

#
# Tests relating to PostgreSQL crash recovery and redo
#
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use IPC::Run;

my $reached_eow_pat = "reached end of WAL at ";
my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->start;

my ($stdin, $stdout, $stderr) = ('', '', '');

# Ensure that pg_xact_status reports 'aborted' for xacts
# that were in-progress during crash. To do that, we need
# an xact to be in-progress when we crash and we need to know
# its xid.
my $tx = IPC::Run::start(
	[
		'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d',
		$node->connstr('postgres')
	],
	'<',
	\$stdin,
	'>',
	\$stdout,
	'2>',
	\$stderr);
$stdin .= q[
BEGIN;
CREATE TABLE mine(x integer);
SELECT pg_current_xact_id();
];
$tx->pump until $stdout =~ /[[:digit:]]+[\r\n]$/;

# Status should be in-progress
my $xid = $stdout;
chomp($xid);

is($node->safe_psql('postgres', qq[SELECT pg_xact_status('$xid');]),
	'in progress', 'own xid is in-progress');

# Crash and restart the postmaster
$node->stop('immediate');
my $logstart = get_log_size($node);
$node->start;
$node->wait_for_log($reached_eow_pat, $logstart);
pass("end-of-wal is logged");

# Make sure we really got a new xid
cmp_ok($node->safe_psql('postgres', 'SELECT pg_current_xact_id()'),
	'>', $xid, 'new xid after restart is greater');

# and make sure we show the in-progress xact as aborted
is($node->safe_psql('postgres', qq[SELECT pg_xact_status('$xid');]),
	'aborted', 'xid is aborted after crash');

$stdin .= "\\q\n";
$tx->finish;    # wait for psql to quit gracefully

my $segsize = $node->safe_psql('postgres',
	   qq[SELECT setting FROM pg_settings WHERE name = 'wal_segment_size';]);

# make sure no records afterwards go to the next segment
$node->safe_psql('postgres', qq[
				 SELECT pg_switch_wal();
				 CHECKPOINT;
				 CREATE TABLE t();
]);
$node->stop('immediate');

# identify REDO WAL file
my $cmd = "pg_controldata -D " . $node->data_dir();
$cmd = ['pg_controldata', '-D', $node->data_dir()];
$stdout = '';
$stderr = '';
IPC::Run::run $cmd, '>', \$stdout, '2>', \$stderr;
ok($stdout =~ /^Latest checkpoint's REDO WAL file:[ \t] *(.+)$/m,
   "checkpoint file is identified");
my $chkptfile = $1;

# identify the last record
my $walfile = $node->data_dir() . "/pg_wal/$chkptfile";
$cmd = ['pg_waldump', $walfile];
$stdout = '';
$stderr = '';
my $lastrec;
IPC::Run::run $cmd, '>', \$stdout, '2>', \$stderr;
foreach my $l (split(/\r?\n/, $stdout))
{
	$lastrec = $l;
}
ok(defined $lastrec, "last WAL record is extracted");
ok($stderr =~ /end of WAL at ([0-9A-F\/]+): .* at \g1/,
   "pg_waldump emits the correct ending message");

# read the last record LSN excluding leading zeroes
ok ($lastrec =~ /, lsn: 0\/0*([1-9A-F][0-9A-F]+),/,
	"LSN of the last record identified");
my $lastlsn = $1;

# corrupt the last record
my $offset = hex($lastlsn) % $segsize;
open(my $segf, '+<', $walfile) or die "failed to open $walfile\n";
seek($segf, $offset, 0);  # halfway corrupt the last record
print $segf "\0\0\0\0";
close($segf);

# pg_waldump complains about the corrupted record
$stdout = '';
$stderr = '';
IPC::Run::run $cmd, '>', \$stdout, '2>', \$stderr;
ok($stderr =~ /error: error in WAL record at 0\/$lastlsn: .* at 0\/$lastlsn/,
   "pg_waldump emits the correct error message");

# also server complains
$logstart = get_log_size($node);
$node->start;
$node->wait_for_log("WARNING:  invalid record length at 0/$lastlsn: wanted [0-9]+, got 0",
					$logstart);
pass("header error is logged at $lastlsn");

# no end-of-wal message should be seen this time
ok(!find_in_log($node, $reached_eow_pat, $logstart),
   "false log message is not emitted");

$node->stop('immediate');

done_testing();

#### helper routines
# return the size of logfile of $node in bytes
sub get_log_size
{
	my ($node) = @_;

	return (stat $node->logfile)[7];
}

# find $pat in logfile of $node after $off-th byte
sub find_in_log
{
	my ($node, $pat, $off) = @_;

	$off = 0 unless defined $off;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
	return 0 if (length($log) <= $off);

	$log = substr($log, $off);

	return $log =~ m/$pat/;
}
