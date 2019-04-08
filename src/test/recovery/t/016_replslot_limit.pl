# Test for replication slot limit
# Ensure that max_slot_wal_keep_size limits the number of WAL files to
# be kept by replication slots.

use strict;
use warnings;
use File::Path qw(rmtree);
use PostgresNode;
use TestLib;
use Test::More tests => 11;
use Time::HiRes qw(usleep);

$ENV{PGDATABASE} = 'postgres';

# Initialize master node, setting wal-segsize to 1MB
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1, extra => ['--wal-segsize=1']);
$node_master->append_conf('postgresql.conf', qq(
min_wal_size = 2MB
max_wal_size = 3MB
));
$node_master->start;
$node_master->safe_psql('postgres', "SELECT pg_create_physical_replication_slot('rep1')");

# The slot state should be the state "unknown" before the first connection
my $result = $node_master->safe_psql('postgres', "SELECT restart_lsn, wal_status, remain FROM pg_replication_slots WHERE slot_name = 'rep1'");
is($result, "|unknown|0", 'check the state of non-reserved slot is "unknown"');


# Take backup
my $backup_name = 'my_backup';
$node_master->backup($backup_name);

# Create a standby linking to it using the replication slot
my $node_standby = get_new_node('standby_1');
$node_standby->init_from_backup($node_master, $backup_name, has_streaming => 1, primary_slot_name => 'rep1');

$node_standby->start;

# Wait until standby has replayed enough data
my $start_lsn = $node_master->lsn('write');
$node_master->wait_for_catchup($node_standby, 'replay', $start_lsn);

# Stop standby
$node_standby->stop;


# Preparation done, the slot is the state "streaming" now
$result = $node_master->safe_psql('postgres', "SELECT restart_lsn, wal_status, remain FROM pg_replication_slots WHERE slot_name = 'rep1'");
is($result, "$start_lsn|streaming|0", 'check the catching-up state');

# Advance WAL by five segments (= 5MB) on master
advance_wal($node_master, 5);
$node_master->safe_psql('postgres', "CHECKPOINT;");

# The slot is always "safe" when max_slot_wal_keep_size is not set
$result = $node_master->safe_psql('postgres', "SELECT restart_lsn, wal_status, remain FROM pg_replication_slots WHERE slot_name = 'rep1'");
is($result, "$start_lsn|streaming|0", 'check that slot is working');

# The standby can connect to master
$node_standby->start;

$start_lsn = $node_master->lsn('write');
$node_master->wait_for_catchup($node_standby, 'replay', $start_lsn);

$node_standby->stop;

# Set max_slot_wal_keep_size on master
my $max_slot_wal_keep_size_mb = 3;
$node_master->append_conf('postgresql.conf', qq(
max_slot_wal_keep_size = ${max_slot_wal_keep_size_mb}MB
));
$node_master->reload;

# The slot is in safe state. The remaining bytes should be as almost
# (max_slot_wal_keep_size + 1) times large as the segment size
$result = $node_master->safe_psql('postgres', "SELECT restart_lsn, wal_status, pg_size_pretty(remain) as remain FROM pg_replication_slots WHERE slot_name = 'rep1'");
is($result, "$start_lsn|streaming|4096 kB", 'check that max_slot_wal_keep_size is working');

# Advance WAL again then checkpoint
advance_wal($node_master, 2);
$node_master->safe_psql('postgres', "CHECKPOINT;");

# The slot is still working.
$result = $node_master->safe_psql('postgres', "SELECT restart_lsn, wal_status, pg_size_pretty(remain) as remain FROM pg_replication_slots WHERE slot_name = 'rep1'");
is($result, "$start_lsn|streaming|2048 kB", 'check that remaining byte is calculated correctly');

# wal_keep_segments overrides max_slot_wal_keep_size
$result = $node_master->safe_psql('postgres', "ALTER SYSTEM SET wal_keep_segments to 6; SELECT pg_reload_conf();");
$result = $node_master->safe_psql('postgres', "SELECT restart_lsn, wal_status, pg_size_pretty(remain) as remain FROM pg_replication_slots WHERE slot_name = 'rep1'");
is($result, "$start_lsn|streaming|5120 kB", 'check that wal_keep_segments overrides max_slot_wal_keep_size');

# restore wal_keep_segments
$result = $node_master->safe_psql('postgres', "ALTER SYSTEM SET wal_keep_segments to 0; SELECT pg_reload_conf();");

# Advance WAL again without checkpoint
advance_wal($node_master, 2);

# Slot gets to 'keeping' state
$result = $node_master->safe_psql('postgres', "SELECT restart_lsn, wal_status, pg_size_pretty(remain) as remain FROM pg_replication_slots WHERE slot_name = 'rep1'");
is($result, "$start_lsn|keeping|0 bytes", 'check that the slot state changes to "keeping"');

# The standby still can connect to master
$node_standby->start;

$start_lsn = $node_master->lsn('write');
$node_master->wait_for_catchup($node_standby, 'replay', $start_lsn);

$node_standby->stop;

ok(!find_in_log($node_standby,
				"requested WAL segment [0-9A-F]+ has already been removed"),
   'check that required WAL segments are still available');

# Advance WAL again, the slot loses some segments.
my $logstart = get_log_size($node_master);
advance_wal($node_master, 5);
$node_master->safe_psql('postgres', "CHECKPOINT;");

# WARNING should be issued
ok(find_in_log($node_master,
			   "some replication slots have lost required WAL segments\n".
			   ".*Slot rep1 lost 2 segment\\(s\\)\\.",
			   $logstart),
   'check that the warning is logged');

# This slot should be broken
$result = $node_master->safe_psql('postgres', "SELECT restart_lsn, wal_status, pg_size_pretty(remain) as remain FROM pg_replication_slots WHERE slot_name = 'rep1'");
is($result, "$start_lsn|lost|0 bytes", 'check that the slot state changes to "lost"');

# The standby no longer can connect to the master
$logstart = get_log_size($node_standby);
$node_standby->start;

my $failed = 0;
for (my $i = 0 ; $i < 10000 ; $i++)
{
	if (find_in_log($node_standby,
					"requested WAL segment [0-9A-F]+ has already been removed",
					$logstart))
	{
		$failed = 1;
		last;
	}
	usleep(100_000);
}
ok($failed, 'check that replication has been broken');

$node_standby->stop;

#####################################
# Advance WAL of $node by $n segments
sub advance_wal
{
	my ($node, $n) = @_;

	# Advance by $n segments (= (16 * $n) MB) on master
	for (my $i = 0 ; $i < $n ; $i++)
	{
		$node->safe_psql('postgres', "CREATE TABLE t (); DROP TABLE t; SELECT pg_switch_wal();");
	}
}

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
	my $log = TestLib::slurp_file($node->logfile);
	return 0 if (length($log) <= $off);

	$log = substr($log, $off);

	return $log =~ m/$pat/;
}
