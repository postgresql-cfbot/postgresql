
# Copyright (c) 2021-2023, PostgreSQL Global Development Group

# Minimal test testing recovery process

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

my ($stdout, $stderr) = ('', '');

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
seek($segf, $offset, 0);  # zero xl_tot_len, leaving following bytes alone.
print $segf "\0\0\0\0";
close($segf);

# pg_waldump complains about the corrupted record
$stdout = '';
$stderr = '';
IPC::Run::run $cmd, '>', \$stdout, '2>', \$stderr;
ok($stderr =~ /error: error in WAL record at 0\/$lastlsn: .* at 0\/$lastlsn/,
   "pg_waldump emits the correct error message");

# also server complains for the same reason
my $logstart = -s $node->logfile;
$node->start;
ok($node->wait_for_log(
	   "WARNING:  invalid record length at 0/$lastlsn: expected at least 24, got 0",
	   $logstart),
   "header error is correctly logged at $lastlsn");

# no end-of-wal message should be seen this time
ok(!find_in_log($node, $reached_eow_pat, $logstart),
   "false log message is not emitted");

# Create streaming standby linking to primary
my $backup_name = 'my_backup';
$node->backup($backup_name);
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node, $backup_name, has_streaming => 1);
$node_standby->start;
$node->safe_psql('postgres', 'CREATE TABLE t ()');
my $primary_lsn = $node->lsn('write');
$node->wait_for_catchup($node_standby, 'write', $primary_lsn);

$node_standby->stop();
$node->stop('immediate');

# crash restart the primary
$logstart = -s $node->logfile;
$node->start();
ok($node->wait_for_log($reached_eow_pat, $logstart),
   'primary properly emits end-of-WAL message');

# restart the standby
$logstart = -s $node_standby->logfile;
$node_standby->start();
ok($node->wait_for_log($reached_eow_pat, $logstart),
   'standby properly emits end-of-WAL message');

$node_standby->stop();
$node->stop();

done_testing();

#### helper routines
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
