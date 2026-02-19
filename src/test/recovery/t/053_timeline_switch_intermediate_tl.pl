
# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Test that archive recovery with three timelines does not fall back to a
# grandparent TL segment when the intermediate TL segment at its own switch
# point is absent from the archive.
#
# Topology: TL1 -> TL2 (switch in seg N+1) -> TL3 (switch in seg N+2).
# The base backup is taken while the primary is still in segment N.
#
# Archive contains TL1 segments N, N+1, N+2 and the TL3 history file, but
# NOT TL2 segment N+1 (the segment containing the TL1->TL2 switch point).
#
# Recovery target is TL3.  For segment N+1, TL2 is the first eligible timeline
# (its begin_seg == N+1).  TL2 segment N+1 is absent, so recovery must stop
# there and not fall back to TL1 segment N+1, which carries divergent WAL from
# the old primary written after the switch point.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Copy;

# Primary (TL1) with WAL archiving.
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1, has_archiving => 1);
$node_primary->start;
$node_primary->safe_psql('postgres', 'CREATE TABLE t (i int)');

# Take the base backup in segment N.  Recovery will start from this backup
# and must replay segment N+1 where the TL2 switch point lands.
$node_primary->backup('primary_backup');

# Switch to segment N+1.  This is where the TL1->TL2 switch point will land.
$node_primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
$node_primary->safe_psql('postgres', 'INSERT INTO t VALUES (1)');

# Capture the TL1 file name for segment N+1.  We will assert later that
# archive recovery did NOT restore this file (the TL1 version carries
# divergent WAL past the switch point).
my $tl2_switch_seg = $node_primary->safe_psql('postgres',
	'SELECT pg_walfile_name(pg_current_wal_lsn())');

# standby1 streams from primary.  Disable archiving so it does not publish
# TL2 segments into the primary's archive, which would mask the bug.
my $node_standby1 = PostgreSQL::Test::Cluster->new('standby1');
$node_standby1->init_from_backup($node_primary, 'primary_backup',
	has_streaming => 1);
$node_standby1->append_conf('postgresql.conf', "archive_mode = off");
$node_standby1->start;
$node_primary->wait_for_catchup($node_standby1);

# Promote standby1 while the primary is writing in segment N+1.  The TL1->TL2
# switch point therefore falls inside segment N+1.
$node_standby1->promote;
$node_standby1->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()')
  or die "Timed out waiting for standby1 promotion to TL2";

# Old primary (TL1) continues writing divergent WAL in segment N+1 and then
# N+2 and archives them.  These are the segments that recovery must not use.
$node_primary->safe_psql('postgres', 'INSERT INTO t VALUES (-1)');
$node_primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
$node_primary->safe_psql('postgres', 'INSERT INTO t VALUES (-2)');
my $tl1_last_seg = $node_primary->safe_psql('postgres',
	'SELECT pg_walfile_name(pg_current_wal_lsn())');
$node_primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
$node_primary->poll_query_until('postgres',
	"SELECT last_archived_wal >= '$tl1_last_seg' FROM pg_stat_archiver")
  or die "Timed out waiting for primary to archive divergent TL1 segments";

# On TL2 (standby1 is now the primary), switch to segment N+2 so the TL2->TL3
# switch point lands one segment further than the TL1->TL2 switch.
$node_standby1->safe_psql('postgres', 'SELECT pg_switch_wal()');
$node_standby1->safe_psql('postgres', 'INSERT INTO t VALUES (2)');

# Take a backup from the TL2 primary for standby2.
$node_standby1->backup('standby1_backup');

# standby2 streams from standby1 (TL2).  Same archiving restriction.
my $node_standby2 = PostgreSQL::Test::Cluster->new('standby2');
$node_standby2->init_from_backup($node_standby1, 'standby1_backup',
	has_streaming => 1);
$node_standby2->append_conf('postgresql.conf', "archive_mode = off");
$node_standby2->start;
$node_standby1->wait_for_catchup($node_standby2);

# Promote standby2 while TL2 is writing in segment N+2.  The TL2->TL3 switch
# point therefore falls inside segment N+2.
$node_standby2->promote;
$node_standby2->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()')
  or die "Timed out waiting for standby2 promotion to TL3";

$node_primary->stop;
$node_standby1->stop;
$node_standby2->stop;

# Copy the TL3 history file to the TL1 archive.  PostgreSQL builds it with
# the full ancestry chain upon promotion:
#   1  <TL2 switch lsn>   (switch from TL1 to TL2 happened in segment N+1)
#   2  <TL3 switch lsn>   (switch from TL2 to TL3 happened in segment N+2)
# TL2 segment N+1 is intentionally absent from the archive: standby1 had
# archive_mode=off so it never wrote TL2 segments there.  Only the TL1
# version of segment N+1 is present.
my $archive = $node_primary->archive_dir;
copy($node_standby2->data_dir . '/pg_wal/00000003.history',
	"$archive/00000003.history")
  or die "Could not copy 00000003.history: $!";

# Build a recovery node from the TL1 base backup, replaying from the TL1
# archive only (no streaming).
my $node_rec = PostgreSQL::Test::Cluster->new('recovering');
$node_rec->init_from_backup($node_primary, 'primary_backup', has_restoring => 1);
$node_rec->enable_restoring($node_primary, 1);
$node_rec->append_conf('postgresql.conf', "recovery_target_timeline = '3'");
$node_rec->start;

$node_rec->poll_query_until('postgres', 'SELECT pg_is_in_recovery()', 't')
  or die "Node is not in recovery";

# With the fix: for segment N+1, TL2 is the first eligible timeline (its
# begin_seg == N+1).  TL2 segment N+1 is absent, so recovery stops and does
# not fall back to the TL1 version.
#
# Without the fix (targetBeginSeg approach): targetBeginSeg == N+2 (TL3's
# begin), and segno N+1 < N+2, so the guard does not fire.  Recovery falls
# through and silently applies TL1 segment N+1, which carries divergent data.
my $log_content = slurp_file($node_rec->logfile);
unlike(
	$log_content,
	qr/restored log file "$tl2_switch_seg"/,
	"archive recovery did not use TL1 segment at TL2 switch point ($tl2_switch_seg)"
);

done_testing();
