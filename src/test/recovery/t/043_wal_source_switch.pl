
# Copyright (c) 2024, PostgreSQL Global Development Group

# Checks for WAL source switch feature.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1, has_archiving => 1);

# Ensure checkpoint doesn't come in our way
$node_primary->append_conf(
	'postgresql.conf', qq(
checkpoint_timeout = 1h
autovacuum = off
));
$node_primary->start;

$node_primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('standby_slot')");

# And some content
$node_primary->safe_psql('postgres',
	"CREATE TABLE tab_int AS SELECT generate_series(1, 10) AS a");

# Take backup
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Create streaming standby from backup
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup(
	$node_primary, $backup_name,
	has_streaming => 1,
	has_restoring => 1);

my $retry_interval = 1;
$node_standby->append_conf(
	'postgresql.conf', qq(
primary_slot_name = 'standby_slot'
streaming_replication_retry_interval = '${retry_interval}s'
log_min_messages = 'debug2'
));
$node_standby->start;

# Wait until standby has replayed enough data
$node_primary->wait_for_catchup($node_standby);

# Generate some data on the primary while the standby is down
$node_standby->stop;
for my $i (1 .. 10)
{
	$node_primary->safe_psql('postgres',
		"INSERT INTO tab_int VALUES (generate_series(11, 20));");
	$node_primary->safe_psql('postgres', "SELECT pg_switch_wal();");
}

# Now wait for replay to complete on standby. We're done waiting when the
# standby has replayed up to the previously saved primary LSN.
my $cur_lsn =
  $node_primary->safe_psql('postgres', "SELECT pg_current_wal_lsn()");

# Generate 1 more WAL file so that we wait predictably for the archiving of
# all WAL files.
$node_primary->advance_wal(1);

my $walfile_name =
  $node_primary->safe_psql('postgres', "SELECT pg_walfile_name('$cur_lsn')");

$node_primary->poll_query_until('postgres',
	"SELECT count(*) = 1 FROM pg_stat_archiver WHERE last_archived_wal = '$walfile_name';"
) or die "Timed out while waiting for archiving of WAL by primary";

my $offset = -s $node_standby->logfile;

# Standby initially fetches WAL from archive after the restart. Since it is
# asked to retry fetching from primary after retry interval
# (i.e. streaming_replication_retry_interval), it will do so. To mimic the
# standby spending some time fetching from archive, we use apply delay
# (i.e. recovery_min_apply_delay) greater than the retry interval, so that for
# fetching the next WAL file the standby honours retry interval and fetches it
# from primary.
my $delay = $retry_interval * 5;
$node_standby->append_conf(
	'postgresql.conf', qq(
recovery_min_apply_delay = '${delay}s'
));
$node_standby->start;

# Wait until standby has replayed enough data
$node_primary->wait_for_catchup($node_standby);

$node_standby->wait_for_log(
	qr/DEBUG: ( [A-Z0-9]+:)? switched WAL source from archive to stream after timeout/,
	$offset);
$node_standby->wait_for_log(
	qr/LOG: ( [A-Z0-9]+:)? started streaming WAL from primary at .* on timeline .*/,
	$offset);

# Check that the data from primary is streamed to standby
my $row_cnt1 =
  $node_primary->safe_psql('postgres', "SELECT count(*) FROM tab_int;");

my $row_cnt2 =
  $node_standby->safe_psql('postgres', "SELECT count(*) FROM tab_int;");
is($row_cnt1, $row_cnt2, 'data from primary is streamed to standby');

done_testing();
