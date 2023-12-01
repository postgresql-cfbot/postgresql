
# Copyright (c) 2023, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Test primary disallowing specified logical replication slots getting ahead of
# specified physical replication slots. It uses the following set up:
#
#				| ----> standby1 (primary_slot_name = sb1_slot)
#				| ----> standby2 (primary_slot_name = sb2_slot)
# primary -----	|
#				| ----> subscriber1 (failover = true)
#				| ----> subscriber2 (failover = false)
#
# standby_slot_names = 'sb1_slot'
#
# Set up is configured in such a way that the logical slot of subscriber1 is
# enabled failover, thus it will wait for the physical slot of
# standby1(sb1_slot) to catch up before sending decoded changes to subscriber1.

# Create primary
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 'logical');

# Configure primary to disallow any logical slots that enabled failover from
# getting ahead of specified physical replication slot (sb1_slot).
$primary->append_conf(
	'postgresql.conf', qq(
standby_slot_names = 'sb1_slot'
));
$primary->start;

$primary->psql('postgres',
	q{SELECT pg_create_physical_replication_slot('sb1_slot');});
$primary->psql('postgres',
	q{SELECT pg_create_physical_replication_slot('sb2_slot');});

$primary->safe_psql('postgres', "CREATE TABLE tab_int (a int PRIMARY KEY);");

my $backup_name = 'backup';
$primary->backup($backup_name);

# Create a standby
my $standby1 = PostgreSQL::Test::Cluster->new('standby1');
$standby1->init_from_backup(
	$primary, $backup_name,
	has_streaming => 1,
	has_restoring => 1);
$standby1->append_conf(
	'postgresql.conf', qq(
primary_slot_name = 'sb1_slot'
));
$standby1->start;
$primary->wait_for_replay_catchup($standby1);

# Create another standby
my $standby2 = PostgreSQL::Test::Cluster->new('standby2');
$standby2->init_from_backup(
	$primary, $backup_name,
	has_streaming => 1,
	has_restoring => 1);
$standby2->append_conf(
	'postgresql.conf', qq(
primary_slot_name = 'sb2_slot'
));
$standby2->start;
$primary->wait_for_replay_catchup($standby2);

# Create publication on primary
my $publisher = $primary;
$publisher->safe_psql('postgres', "CREATE PUBLICATION regress_mypub FOR TABLE tab_int;");
my $publisher_connstr = $publisher->connstr . ' dbname=postgres';

# Create a subscriber node, wait for sync to complete
my $subscriber1 = PostgreSQL::Test::Cluster->new('subscriber1');
$subscriber1->init(allows_streaming => 'logical');
$subscriber1->start;
$subscriber1->safe_psql('postgres', "CREATE TABLE tab_int (a int PRIMARY KEY);");

# Create a subscription with failover = true
$subscriber1->safe_psql('postgres',
		"CREATE SUBSCRIPTION regress_mysub1 CONNECTION '$publisher_connstr' "
	  . "PUBLICATION regress_mypub WITH (slot_name = lsub1_slot, failover = true);");
$subscriber1->wait_for_subscription_sync;

# Create another subscriber node without enabling failover, wait for sync to
# complete
my $subscriber2 = PostgreSQL::Test::Cluster->new('subscriber2');
$subscriber2->init(allows_streaming => 'logical');
$subscriber2->start;
$subscriber2->safe_psql('postgres', "CREATE TABLE tab_int (a int PRIMARY KEY);");
$subscriber2->safe_psql('postgres',
		"CREATE SUBSCRIPTION mysub2 CONNECTION '$publisher_connstr' "
	  . "PUBLICATION regress_mypub WITH (slot_name = lsub2_slot);");
$subscriber2->wait_for_subscription_sync;

# Stop the standby associated with specified physical replication slot so that
# the logical replication slot won't receive changes until the standby comes
# up.
$standby1->stop;

# Create some data on primary
my $primary_row_count = 10;
my $primary_insert_time = time();
$primary->safe_psql('postgres',
	"INSERT INTO tab_int SELECT generate_series(1, $primary_row_count);");

# Wait for the standby that's up and running gets the data from primary
$primary->wait_for_replay_catchup($standby2);
my $result = $standby2->safe_psql('postgres',
	"SELECT count(*) = $primary_row_count FROM tab_int;");
is($result, 't', "standby2 gets data from primary");

# Wait for the subscription that's up and running and is not enabled for failover.
# It gets the data from primary without waiting for any standbys.
$publisher->wait_for_catchup('mysub2');
$result = $subscriber2->safe_psql('postgres',
	"SELECT count(*) = $primary_row_count FROM tab_int;");
is($result, 't', "subscriber2 gets data from primary");

# The subscription that's up and running and is enabled for failover
# doesn't get the data from primary and keeps waiting for the
# standby specified in standby_slot_names.
$result = $subscriber1->safe_psql('postgres',
	"SELECT count(*) = 0 FROM tab_int;");
is($result, 't', "subscriber1 doesn't get data from primary until standby1 acknowledges changes");

# Start the standby specified in standby_slot_names and wait for it to catch
# up with the primary.
$standby1->start;
$primary->wait_for_replay_catchup($standby1);
$result = $standby1->safe_psql('postgres',
	"SELECT count(*) = $primary_row_count FROM tab_int;");
is($result, 't', "standby1 gets data from primary");

# Now that the standby specified in standby_slot_names is up and running,
# primary must send the decoded changes to subscription enabled for failover
# While the standby was down, this subscriber didn't receive any data from
# primary i.e. the primary didn't allow it to go ahead of standby.
$publisher->wait_for_catchup('regress_mysub1');
$result = $subscriber1->safe_psql('postgres',
	"SELECT count(*) = $primary_row_count FROM tab_int;");
is($result, 't', "subscriber1 gets data from primary after standby1 acknowledges changes");

# Test logical failover slots on the standby
# Configure standby3 to replicate and synchronize logical slots configured
# for failover on the primary
#
#              failover slot lsub1_slot->| ----> subscriber1 (connected via logical replication)
# primary --->                           |
#              physical slot sb3_slot--->| ----> standby3 (connected via streaming replication)
#                                        |                 lsub1_slot(synced_slot)

# Cleanup old standby_slot_names
$primary->stop;
$primary->append_conf(
	'postgresql.conf', qq(
standby_slot_names = ''
));
$primary->start;

$primary->psql('postgres',
		q{SELECT pg_create_physical_replication_slot('sb3_slot');});

$backup_name = 'backup2';
$primary->backup($backup_name);

# Create standby3
my $standby3 = PostgreSQL::Test::Cluster->new('standby3');
$standby3->init_from_backup(
	$primary, $backup_name,
	has_streaming => 1,
	has_restoring => 1);

my $connstr_1 = $primary->connstr;
$standby3->stop;
$standby3->append_conf(
	'postgresql.conf', q{
enable_syncslot = true
hot_standby_feedback = on
primary_slot_name = 'sb3_slot'
});
$standby3->append_conf(
        'postgresql.conf', qq(
primary_conninfo = '$connstr_1 dbname=postgres'
));
$standby3->start;

# Add this standby into the primary's configuration
$primary->stop;
$primary->append_conf(
	'postgresql.conf', qq(
standby_slot_names = 'sb3_slot'
));
$primary->start;

# Restart the standby
$standby3->restart;

# Wait for the standby to start sync
my $offset = -s $standby3->logfile;
$standby3->wait_for_log(
	qr/LOG: ( [A-Z0-9]+:)? waiting for remote slot \"lsub1_slot\"/,
	$offset);

# Advance lsn on the primary
$primary->safe_psql('postgres',
	"SELECT pg_log_standby_snapshot();");
$primary->safe_psql('postgres',
	"SELECT pg_log_standby_snapshot();");
$primary->safe_psql('postgres',
	"SELECT pg_log_standby_snapshot();");

# Wait for the standby to finish sync
$offset = -s $standby3->logfile;
$standby3->wait_for_log(
	qr/LOG: ( [A-Z0-9]+:)? wait over for remote slot \"lsub1_slot\"/,
	$offset);

# Confirm that logical failover slot is created on the standby
is( $standby3->safe_psql('postgres',
	q{SELECT slot_name FROM pg_replication_slots;}
  ),
  'lsub1_slot',
  'failover slot was created');

# Verify slot properties on the standby
is( $standby3->safe_psql('postgres',
	q{SELECT failover, sync_state FROM pg_replication_slots WHERE slot_name = 'lsub1_slot';}
  ),
  "t|r",
  'logical slot has sync_state as ready and failover as true on standby');

# Verify slot properties on the primary
is( $primary->safe_psql('postgres',
    q{SELECT failover, sync_state FROM pg_replication_slots WHERE slot_name = 'lsub1_slot';}
  ),
  "t|n",
  'logical slot has sync_state as none and failover as true on primary');

# Test to confirm that restart_lsn of the logical slot on the primary is synced to the standby

# Truncate table on primary
$primary->safe_psql('postgres',
	"TRUNCATE TABLE tab_int;");

# Insert data on the primary
$primary->safe_psql('postgres',
	"INSERT INTO tab_int SELECT generate_series(1, $primary_row_count);");

# let the slots get synced on the standby
sleep 2;

# Get the restart_lsn for the logical slot lsub1_slot on the primary
my $primary_lsn = $primary->safe_psql('postgres',
	"SELECT restart_lsn from pg_replication_slots WHERE slot_name = 'lsub1_slot';");

# Confirm that restart_lsn of lsub1_slot slot is synced to the standby
$result = $standby3->safe_psql('postgres',
	qq[SELECT '$primary_lsn' <= restart_lsn from pg_replication_slots WHERE slot_name = 'lsub1_slot';]);
is($result, 't', 'restart_lsn of slot lsub1_slot synced to standby');

# Get the confirmed_flush_lsn for the logical slot lsub1_slot on the primary
$primary_lsn = $primary->safe_psql('postgres',
	"SELECT confirmed_flush_lsn from pg_replication_slots WHERE slot_name = 'lsub1_slot';");

# Confirm that confirmed_flush_lsn of lsub1_slot slot is synced to the standby
$result = $standby3->safe_psql('postgres',
	qq[SELECT '$primary_lsn' <= confirmed_flush_lsn from pg_replication_slots WHERE slot_name = 'lsub1_slot';]);
is($result, 't', 'confirmed_flush_lsn of slot lsub1_slot synced to the standby');

done_testing();
