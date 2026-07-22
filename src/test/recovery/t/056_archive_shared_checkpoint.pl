# Copyright (c) 2026, PostgreSQL Global Development Group

# Tests for archive_mode=shared correctness on standbys:
#
# 1. Checkpoint on standby must NOT remove WAL segments that have a .ready
#    status file (i.e. not yet archived by the primary).  With the bug,
#    XLogArchiveCheckDone() returns true unconditionally during recovery for
#    any mode that is not "always", so checkpoint deletes these segments.
#
# 2. After archiving is broken on the primary and then restored, .ready files
#    on the standby must eventually transition to .done (primary sends archival
#    status reports to the standby via the walsender).

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Use 1 MB WAL segments so we can generate many segments cheaply.
my $wal_segsize = 1;

# An archive command that always fails (but is recognized by the archiver as a
# real failure, not a missing command).  Mirrors the approach in
# 020_archive_status.pl to stay portable.
my $broken_command =
  $PostgreSQL::Test::Utils::windows_os
  ? q{copy "%p_does_not_exist" "%f_does_not_exist"}
  : q{cp "%p_does_not_exist" "%f_does_not_exist"};

my $archive_dir = PostgreSQL::Test::Utils::tempdir();
my $good_command =
  $PostgreSQL::Test::Utils::windows_os
  ? qq{copy "%p" "$archive_dir\\%f"}
  : qq{cp %p "$archive_dir/%f"};

###############################################################################
# Set up primary with archive_mode=shared and BROKEN archiving so that every
# WAL segment received by the standby gets a .ready file.
###############################################################################

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(
	has_archiving   => 1,
	allows_streaming => 1,
	extra           => [ '--wal-segsize' => $wal_segsize ]);
$primary->append_conf('postgresql.conf', qq{
archive_mode    = shared
archive_status_report_interval = 10ms
archive_command = '$broken_command'
wal_keep_size   = 0 # to trigger wal deletion
});
$primary->start;

my $backup_name = 'standby_backup';
$primary->backup($backup_name);

my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, $backup_name, has_streaming => 1);
$standby->append_conf('postgresql.conf', qq{
archive_mode               = shared
archive_status_report_interval = 10ms
archive_command            = '$good_command'
wal_receiver_status_interval = 1s
wal_keep_size              = 0 # to trigger wal deletion
});
$standby->start;

$primary->wait_for_catchup($standby);

###############################################################################
# Generate WAL while archiving is broken.
# The standby will create .ready files for every received segment.
###############################################################################

# Switch WAL several times to create clearly-identifiable old segments.
# We capture the name of the first switched-away segment; it is the primary
# candidate that checkpoint would delete.
my $target_seg = $primary->safe_psql('postgres',
	q{SELECT pg_walfile_name(pg_current_wal_lsn())});

for my $i (1..3)
{
	$primary->safe_psql('postgres', "SELECT txid_current();SELECT pg_switch_wal();");
}

# Wait for the archiver to register failures so we are sure archiving is
# truly broken (not just slow).
$primary->poll_query_until('postgres',
	q{SELECT failed_count > 0 FROM pg_stat_archiver})
  or die "Timed out waiting for archiver to fail";

# Issue a CHECKPOINT on the primary so that the standby can form a
# restartpoint whose redo LSN is past $target_seg.
$primary->safe_psql('postgres', 'CHECKPOINT');

# Wait for the standby to replay everything up to that checkpoint.
$primary->wait_for_catchup($standby);

my $standby_wal_dir    = $standby->data_dir . '/pg_wal';
my $standby_status_dir = "$standby_wal_dir/archive_status";

# The target segment must already be visible on the standby as .ready.
my $target_ready = "$standby_status_dir/$target_seg.ready";
ok(-f $target_ready,
	"standby has .ready file for segment $target_seg (not archived by primary)");

# The WAL file itself must also be present.
ok(-f "$standby_wal_dir/$target_seg",
	"WAL segment $target_seg exists in standby pg_wal before CHECKPOINT");

###############################################################################
# Test 1: CHECKPOINT (restartpoint) on standby must not remove .ready segments
###############################################################################

# This triggers CreateRestartPoint, which calls RemoveOldXlogFiles.
# With the bug, XLogArchiveCheckDone returns true for every segment in
# archive_mode=shared during recovery, so $target_seg would be deleted.
$standby->safe_psql('postgres', 'CHECKPOINT');

ok(-f "$standby_wal_dir/$target_seg",
	"WAL segment $target_seg still exists after CHECKPOINT on standby "
	  . "(not deleted despite .ready status)");

ok(-f $target_ready,
	".ready file for $target_seg still present after CHECKPOINT on standby");

###############################################################################
# Test 2: Restoring archiving on primary causes .ready -> .done on standby
#
# This part is independent of Test 1: we generate fresh WAL (with archiving
# still broken) so the standby accumulates new .ready files, then restore
# archiving and verify those files become .done.
###############################################################################

# Generate a few more segments so the standby definitely has fresh .ready files
# regardless of what checkpoint may have done above.
for my $i (1..3)
{
	$primary->safe_psql('postgres', "SELECT txid_current();SELECT pg_switch_wal();");
}
$primary->wait_for_catchup($standby);

# Collect all current .ready files on the standby.
my @ready_segs;
if (opendir(my $dh, $standby_status_dir))
{
	@ready_segs =
	  map { (my $s = $_) =~ s/\.ready$//; $s }
	  grep { /\.ready$/ } readdir($dh);
	closedir($dh);
}
note("Standby has "
	  . scalar(@ready_segs)
	  . " .ready segments before archiving is restored");
cmp_ok(scalar(@ready_segs), '>', 0,
	"standby has fresh .ready files for newly received unarchived segments");

# Restore archiving on the primary.
$primary->safe_psql('postgres', qq{
	ALTER SYSTEM SET archive_command TO '$good_command';
	SELECT pg_reload_conf();
});

# Wait until primary has archived at least one segment.
$primary->poll_query_until('postgres',
	q{SELECT archived_count > 0 FROM pg_stat_archiver})
  or die "Timed out waiting for primary to start archiving after restore";

# Generate one more WAL switch so the walsender picks up the updated
# last_archived_wal and sends a fresh archival report to the standby.
# (The walsender only sends when last_archived_wal changes and every
# archive_status_report_interval = 10 ms at most.)
$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
$primary->wait_for_catchup($standby);

# Poll until all previously .ready segments have become .done.
# Allow up to the framework default timeout (usually 120 s); the walsender
# reports every 10 ms so convergence should happen well within that.
my $remaining_ready = scalar(@ready_segs);
for my $i (1 .. $PostgreSQL::Test::Utils::timeout_default)
{
	$remaining_ready = 0;
	if (opendir(my $dh, $standby_status_dir))
	{
		# Count only the segments that were .ready before archiving was restored
		for my $seg (@ready_segs)
		{
			$remaining_ready++ if -f "$standby_status_dir/$seg.ready";
		}
		closedir($dh);
	}
	last if $remaining_ready == 0;
	sleep(1);
}

for my $seg (@ready_segs)
{
	ok( -f "$standby_status_dir/$seg.done",
		"$seg .ready file on standby transitioned to .done"
	);
}

is($remaining_ready, 0,
	"all .ready files on standby transitioned to .done "
	  . "after archiving restored on primary");

# Sanity-check: the WAL files are still present (they weren't deleted by
# checkpoint while .ready, nor disappeared otherwise).
my @still_missing = grep { !-f "$standby_wal_dir/$_" } @ready_segs;
is(scalar(@still_missing), 0,
	"WAL segments were not lost while waiting for archival reports");

done_testing();
