
# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Test that archive recovery with recovery_target_timeline='latest' does not
# use a parent timeline's WAL segment for the segment containing the switch
# point when the child timeline's segment is absent from the archive.
#
# Setup: TL1 archives segments 1..3 (segment 3 has data past the switch point).
# Only the TL2 timeline history file is added to archive; TL2 segment 3 is not.
# Recovery is performed with recovery_target_timeline = '2'.  Without the fix,
# recovery uses TL1 segment 3 even though the switch point is in TL2 segment 3.
# With the fix, recovery skips TL1 for that segment and correctly waits for TL2.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Copy;

# Initialize primary with WAL archiving.
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1, has_archiving => 1);
$node_primary->start;

$node_primary->safe_psql('postgres', 'CREATE TABLE t (i int)');
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Create a streaming standby so we can promote it.  Disable archiving so
# it does not inherit the primary's archive_command and archive TL2 segments
# into the primary's archive (which would mask the bug by making TL2 seg 3
# available when it should not be).
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name, has_streaming => 1);
$node_standby->append_conf('postgresql.conf', "archive_mode = off");
$node_standby->start;
$node_primary->wait_for_catchup($node_standby);

# Force a segment boundary: switch to segment 3, so the switch point will
# land inside segment 3 (both TL1 and TL2 will have a segment 3).
$node_primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
$node_primary->safe_psql('postgres', 'INSERT INTO t VALUES (1)');
$node_primary->wait_for_catchup($node_standby);

# Promote standby to TL2.  The timeline history file (00000002.history) is
# written to pg_wal immediately upon promotion.
$node_standby->promote;
$node_standby->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()')
  or die "Timed out waiting for promotion";

# Old primary writes to segment 3 and archives it.  This segment overlaps
# the switch point but is on TL1 -- recovery must NOT use it for TL2.
$node_primary->safe_psql('postgres', 'INSERT INTO t VALUES (-1)');
my $old_walfile = $node_primary->safe_psql('postgres',
	'SELECT pg_walfile_name(pg_current_wal_lsn())');
$node_primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
$node_primary->poll_query_until('postgres',
	"SELECT last_archived_wal >= '$old_walfile' FROM pg_stat_archiver")
  or die "Timed out waiting for old primary to archive";

# Add TL2 timeline history to the archive by copying it directly from the
# standby's pg_wal.  This tells recovery that TL2 exists, but TL2 segment 3
# is intentionally absent so recovery must not fall back to TL1 segment 3.
my $archive = $node_primary->archive_dir;
copy($node_standby->data_dir . '/pg_wal/00000002.history',
	"$archive/00000002.history")
  or die "Could not copy 00000002.history: $!";

# Stop both nodes.  Recovery will run archive-only, no streaming source.
$node_primary->stop;
$node_standby->stop;

# Create a recovery node using old primary's archive only (no streaming).
my $node_rec = PostgreSQL::Test::Cluster->new('recovering');
$node_rec->init_from_backup($node_primary, $backup_name, has_restoring => 1);
$node_rec->enable_restoring($node_primary, 1);
$node_rec->append_conf('postgresql.conf', "recovery_target_timeline = '2'");

$node_rec->start;

# Give recovery a moment to attempt restoring the switch-point segment.
$node_rec->poll_query_until('postgres', 'SELECT pg_is_in_recovery()', 't')
  or die "Node is not in recovery";

# With the fix: recovery skips TL1 for the switch-point segment and waits
# for TL2 (which is absent).  Without the fix: it restores TL1 segment 3.
my $log_content = slurp_file($node_rec->logfile);
unlike($log_content, qr/restored log file "000000010000000000000003"/,
	'archive recovery did not use TL1 segment 3 past the switch point');

done_testing();
