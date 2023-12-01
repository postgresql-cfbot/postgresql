# Test that setting up and starting WAL archiving on an
# already-promoted node will result in the archival of its current
# timeline history file.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

$ENV{PGDATABASE} = 'postgres';

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->start;

# Take a backup
my $backup_name = 'my_backup_1';
$node_primary->backup($backup_name);

# Create a standby that will be promoted onto timeline 2
my $node_primary_tli2 = PostgreSQL::Test::Cluster->new('primary_tli2');
$node_primary_tli2->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_primary_tli2->start;

# Stop and remove the primary; it's not needed anymore
$node_primary->teardown_node;

# Promote the standby using "pg_promote", switching it to timeline 2
my $psql_out = '';
$node_primary_tli2->psql(
	'postgres',
	"SELECT pg_promote(wait_seconds => 300);",
	stdout => \$psql_out);
is($psql_out, 't', "promotion of standby with pg_promote");

# Enable archiving on the promoted node.
$node_primary_tli2->enable_archiving;
$node_primary_tli2->restart;

# Check that the timeline 2 history file was not archived after
# enabling WAL archiving since timeline history files are only
# archived at the moment of switching timelines and not any time
# after.
my $primary_tli2_archive = $node_primary_tli2->archive_dir;
my $primary_tli2_datadir = $node_primary_tli2->data_dir;
ok(-f "$primary_tli2_datadir/pg_wal/00000002.history",
   'timeline 2 history file was created');
ok(! -f "$primary_tli2_datadir/pg_wal/archive_status/00000002.history.ready",
   'timeline 2 history file was not marked for WAL archiving');
ok(! -f "$primary_tli2_datadir/pg_wal/archive_status/00000002.history.done",
   'timeline 2 history file was not archived archived');
ok(! -f "$primary_tli2_archive/00000002.history",
   'timeline 2 history file does not exist in the archive');

# Take backup of node_primary_tli2 and use -Xnone so that pg_wal will
# be empty and restore will retrieve the necessary WAL and timeline
# history file(s) from the archive.
$backup_name = 'my_backup_2';
$node_primary_tli2->backup($backup_name, backup_options => ['-Xnone']);

# Create simple WAL that will be archived and restored
$node_primary_tli2->safe_psql('postgres', "CREATE TABLE tab_int AS SELECT 8 AS a;");

# Create a restore point to later use as the recovery_target_name
my $recovery_name = "my_target";
$node_primary_tli2->safe_psql('postgres',
	"SELECT pg_create_restore_point('$recovery_name');");

# Find the next WAL segment to be archived
my $walfile_to_be_archived = $node_primary_tli2->safe_psql('postgres',
	"SELECT pg_walfile_name(pg_current_wal_lsn());");

# Make the WAL segment eligible for archival
$node_primary_tli2->safe_psql('postgres', 'SELECT pg_switch_wal();');

# Wait until the WAL segment has been archived
my $archive_wait_query =
  "SELECT '$walfile_to_be_archived' <= last_archived_wal FROM pg_stat_archiver;";
$node_primary_tli2->poll_query_until('postgres', $archive_wait_query)
  or die "Timed out while waiting for WAL segment to be archived";
$node_primary_tli2->teardown_node;

# Initialize a new standby node from the backup. This node will start
# off on timeline 2 according to the control file and will finish
# recovery onto the same timeline by explicitly setting
# recovery_target_timeline to '2'. We explicitly set the target
# timeline to show that it doesn't require the timeline history file
# and works the same as if we used 'current' or 'latest'.
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary_tli2, $backup_name,
	has_restoring => 1, standby => 0);
$node_standby->append_conf('postgresql.conf', qq{
recovery_target_timeline = '2'
recovery_target_action = 'pause'
recovery_target_name = 'my_target'
archive_mode = 'off'
primary_conninfo = ''
});
$node_standby->start;

# Check that the timeline history file was not retrieved
ok ( ! -f $node_standby->data_dir . "/pg_wal/00000002.history",
  "00000002.history does not exist in the standby's pg_wal directory");

# Sanity check that the node is queryable
my $result_standby =
  $node_standby->safe_psql('postgres', "SELECT timeline_id FROM pg_control_checkpoint();");
is($result_standby, qq(2), 'check that the node is on timeline 2');
$result_standby =
  $node_standby->safe_psql('postgres', "SELECT * FROM tab_int;");
is($result_standby, qq(8), 'check that the node did archive recovery');

# Set up a cascade standby node to validate that there's no issues
# since the WAL receiver will request all necessary timeline history
# files from the standby node's WAL sender.
my $node_cascade = PostgreSQL::Test::Cluster->new('cascade');
$node_cascade->init_from_backup($node_primary_tli2, $backup_name,
	standby => 1);
$node_cascade->enable_streaming($node_standby);
$node_cascade->start;

# Wait for the replication to catch up
$node_standby->wait_for_catchup($node_cascade);

# Sanity check that the cascade standby node came up and is queryable
my $result_cascade =
  $node_cascade->safe_psql('postgres', "SELECT * FROM tab_int;");
is($result_cascade, qq(8), 'check that the node received the streamed WAL data');

$node_standby->teardown_node;
$node_cascade->teardown_node;

done_testing();
