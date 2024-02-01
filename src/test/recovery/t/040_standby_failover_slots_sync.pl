
# Copyright (c) 2024, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

##################################################
# Test that when a subscription with failover enabled is created, it will alter
# the failover property of the corresponding slot on the publisher.
##################################################

# Create publisher
my $publisher = PostgreSQL::Test::Cluster->new('publisher');
$publisher->init(allows_streaming => 'logical');
$publisher->start;

$publisher->safe_psql('postgres',
	"CREATE PUBLICATION regress_mypub FOR ALL TABLES;"
);

my $publisher_connstr = $publisher->connstr . ' dbname=postgres';

# Create a subscriber node, wait for sync to complete
my $subscriber1 = PostgreSQL::Test::Cluster->new('subscriber1');
$subscriber1->init;
$subscriber1->start;

# Create a slot on the publisher with failover disabled
$publisher->safe_psql('postgres',
	"SELECT 'init' FROM pg_create_logical_replication_slot('lsub1_slot', 'pgoutput', false, false, false);"
);

# Confirm that the failover flag on the slot is turned off
is( $publisher->safe_psql(
		'postgres',
		q{SELECT failover from pg_replication_slots WHERE slot_name = 'lsub1_slot';}
	),
	"f",
	'logical slot has failover false on the publisher');

# Create a subscription (using the same slot created above) that enables
# failover.
$subscriber1->safe_psql('postgres',
	"CREATE SUBSCRIPTION regress_mysub1 CONNECTION '$publisher_connstr' PUBLICATION regress_mypub WITH (slot_name = lsub1_slot, copy_data=false, failover = true, create_slot = false, enabled = false);"
);

# Confirm that the failover flag on the slot has now been turned on
is( $publisher->safe_psql(
		'postgres',
		q{SELECT failover from pg_replication_slots WHERE slot_name = 'lsub1_slot';}
	),
	"t",
	'logical slot has failover true on the publisher');

##################################################
# Test that changing the failover property of a subscription updates the
# corresponding failover property of the slot.
##################################################

# Disable failover
$subscriber1->safe_psql('postgres',
	"ALTER SUBSCRIPTION regress_mysub1 SET (failover = false)");

# Confirm that the failover flag on the slot has now been turned off
is( $publisher->safe_psql(
		'postgres',
		q{SELECT failover from pg_replication_slots WHERE slot_name = 'lsub1_slot';}
	),
	"f",
	'logical slot has failover false on the publisher');

# Enable failover
$subscriber1->safe_psql('postgres',
	"ALTER SUBSCRIPTION regress_mysub1 SET (failover = true)");

# Confirm that the failover flag on the slot has now been turned on
is( $publisher->safe_psql(
		'postgres',
		q{SELECT failover from pg_replication_slots WHERE slot_name = 'lsub1_slot';}
	),
	"t",
	'logical slot has failover true on the publisher');

##################################################
# Test that the failover option cannot be changed for enabled subscriptions.
##################################################

# Enable subscription
$subscriber1->safe_psql('postgres',
	"ALTER SUBSCRIPTION regress_mysub1 ENABLE");

# Disable failover for enabled subscription
my ($result, $stdout, $stderr) = $subscriber1->psql('postgres',
	"ALTER SUBSCRIPTION regress_mysub1 SET (failover = false)");
ok( $stderr =~ /ERROR:  cannot set failover for enabled subscription/,
	"altering failover is not allowed for enabled subscription");

##################################################
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
##################################################

# Create primary
my $primary = $publisher;

$primary->psql('postgres',
	q{SELECT pg_create_physical_replication_slot('sb1_slot');});
$primary->psql('postgres',
	q{SELECT pg_create_physical_replication_slot('sb2_slot');});

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

# Configure primary to disallow any logical slots that enabled failover from
# getting ahead of specified physical replication slot (sb1_slot).
$primary->append_conf(
	'postgresql.conf', qq(
standby_slot_names = 'sb1_slot'
));
$primary->reload;

$primary->safe_psql('postgres', "CREATE TABLE tab_int (a int PRIMARY KEY);");

# Create a table and refresh the publication
$subscriber1->safe_psql(
	'postgres', qq[
	CREATE TABLE tab_int (a int PRIMARY KEY);
	ALTER SUBSCRIPTION regress_mysub1 REFRESH PUBLICATION WITH (copy_data = false);
]);

# Create another subscriber node without enabling failover, wait for sync to
# complete
my $subscriber2 = PostgreSQL::Test::Cluster->new('subscriber2');
$subscriber2->init;
$subscriber2->start;
$subscriber2->safe_psql(
	'postgres', qq[
	CREATE TABLE tab_int (a int PRIMARY KEY);
	CREATE SUBSCRIPTION regress_mysub2 CONNECTION '$publisher_connstr' PUBLICATION regress_mypub WITH (slot_name = lsub2_slot, copy_data = false);
]);

# Stop the standby associated with the specified physical replication slot so
# that the logical replication slot won't receive changes until the standby
# comes up.
$standby1->stop;

# Create some data on the primary
my $primary_row_count = 10;
$primary->safe_psql('postgres',
	"INSERT INTO tab_int SELECT generate_series(1, $primary_row_count);");

# Wait for the standby that's up and running gets the data from primary
$primary->wait_for_replay_catchup($standby2);
$result = $standby2->safe_psql('postgres',
	"SELECT count(*) = $primary_row_count FROM tab_int;");
is($result, 't', "standby2 gets data from primary");

# Wait for the subscription that's up and running and is not enabled for failover.
# It gets the data from primary without waiting for any standbys.
$publisher->wait_for_catchup('regress_mysub2');
$result = $subscriber2->safe_psql('postgres',
	"SELECT count(*) = $primary_row_count FROM tab_int;");
is($result, 't', "subscriber2 gets data from primary");

# The subscription that's up and running and is enabled for failover
# doesn't get the data from primary and keeps waiting for the
# standby specified in standby_slot_names.
$result =
  $subscriber1->safe_psql('postgres', "SELECT count(*) = 0 FROM tab_int;");
is($result, 't',
	"subscriber1 doesn't get data from primary until standby1 acknowledges changes"
);

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
is($result, 't',
	"subscriber1 gets data from primary after standby1 acknowledges changes");

# Stop the standby associated with the specified physical replication slot so
# that the logical replication slot won't receive changes until the standby
# slot's restart_lsn is advanced or the slot is removed from the
# standby_slot_names list.
$publisher->safe_psql('postgres', "TRUNCATE tab_int;");
$publisher->wait_for_catchup('regress_mysub1');
$standby1->stop;

##################################################
# Verify that when using pg_logical_slot_get_changes to consume changes from a
# logical slot with failover enabled, it will also wait for the slots specified
# in standby_slot_names to catch up.
##################################################

# Create a logical 'test_decoding' replication slot with failover enabled
$publisher->safe_psql('postgres',
	"SELECT pg_create_logical_replication_slot('test_slot', 'test_decoding', false, false, true);"
);

my $back_q = $primary->background_psql('postgres', on_error_stop => 0);
my $pid = $back_q->query('SELECT pg_backend_pid()');

# Try and get changes from the logical slot with failover enabled.
my $offset = -s $primary->logfile;
$back_q->query_until(qr//,
	"SELECT pg_logical_slot_get_changes('test_slot', NULL, NULL);\n");

# Wait until the primary server logs a warning indicating that it is waiting
# for the sb1_slot to catch up.
$primary->wait_for_log(
	qr/WARNING: ( [A-Z0-9]+:)? replication slot \"sb1_slot\" specified in parameter \"standby_slot_names\" does not have active_pid/,
	$offset);

ok($primary->safe_psql('postgres', "SELECT pg_cancel_backend($pid)"),
	"cancelling pg_logical_slot_get_changes command");

$back_q->quit;

$publisher->safe_psql('postgres',
	"SELECT pg_drop_replication_slot('test_slot');"
);

##################################################
# Test that logical replication will wait for the user-created inactive
# physical slot to catch up until we remove the slot from standby_slot_names.
##################################################

# Create some data on the primary
$primary_row_count = 10;
$primary->safe_psql('postgres',
	"INSERT INTO tab_int SELECT generate_series(1, $primary_row_count);");

$result =
  $subscriber1->safe_psql('postgres', "SELECT count(*) = 0 FROM tab_int;");
is($result, 't',
	"subscriber1 doesn't get data as the sb1_slot doesn't catch up");

# Remove the standby from the standby_slot_names list and reload the
# configuration.
$primary->adjust_conf('postgresql.conf', 'standby_slot_names', "''");
$primary->reload;

# Since there are no slots in standby_slot_names, the primary server should now
# send the decoded changes to the subscription.
$publisher->wait_for_catchup('regress_mysub1');
$result = $subscriber1->safe_psql('postgres',
	"SELECT count(*) = $primary_row_count FROM tab_int;");
is($result, 't',
	"subscriber1 gets data from primary after standby1 is removed from the standby_slot_names list"
);

# Put the standby back on the primary_slot_name for the rest of the tests
$primary->adjust_conf('postgresql.conf', 'standby_slot_names', 'sb1_slot');
$primary->reload;

##################################################
# Test logical failover slots on the standby
# Configure standby1 to replicate and synchronize logical slots configured
# for failover on the primary
#
#              failover slot lsub1_slot->| ----> subscriber1 (connected via logical replication)
# primary --->                           |
#              physical slot sb1_slot--->| ----> standby1 (connected via streaming replication)
#                                        |                 lsub1_slot(synced_slot)
##################################################

# Create a standby
my $connstr_1 = $primary->connstr;
$standby1->append_conf(
	'postgresql.conf', qq(
enable_syncslot = true
hot_standby_feedback = on
primary_conninfo = '$connstr_1 dbname=postgres'
));

my $standby1_conninfo = $standby1->connstr . ' dbname=postgres';
$offset = -s $standby1->logfile;

# Start the standby so that slot syncing can begin
$standby1->start;

# Generate a log to trigger the walsender to send messages to the walreceiver
# which will update WalRcv->latestWalEnd to a valid number.
$primary->safe_psql('postgres', "SELECT pg_log_standby_snapshot();");

# Wait for the standby to finish sync
$standby1->wait_for_log(
	qr/LOG: ( [A-Z0-9]+:)? newly created slot \"lsub1_slot\" is sync-ready now/,
	$offset);

# Confirm that the logical failover slot is created on the standby and is
# flagged as 'synced'
is($standby1->safe_psql('postgres',
	q{SELECT synced FROM pg_replication_slots WHERE slot_name = 'lsub1_slot';}),
	"t",
	'logical slot has synced as true on standby');

##################################################
# Test to confirm that restart_lsn and confirmed_flush_lsn of the logical slot
# on the primary is synced to the standby
##################################################

# Insert data on the primary
$primary->safe_psql(
	'postgres', qq[
	TRUNCATE TABLE tab_int;
	INSERT INTO tab_int SELECT generate_series(1, 10);
]);

$primary->wait_for_catchup('regress_mysub1');

# Do not allow any further advancement of the restart_lsn and
# confirmed_flush_lsn for the lsub1_slot.
$subscriber1->safe_psql('postgres', "ALTER SUBSCRIPTION regress_mysub1 DISABLE");

# Wait for the replication slot to become inactive on the publisher
$primary->poll_query_until(
	'postgres',
	"SELECT COUNT(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = 'lsub1_slot' AND active='f'",
	1);

# Get the restart_lsn for the logical slot lsub1_slot on the primary
my $primary_restart_lsn = $primary->safe_psql('postgres',
	"SELECT restart_lsn from pg_replication_slots WHERE slot_name = 'lsub1_slot';");

# Get the confirmed_flush_lsn for the logical slot lsub1_slot on the primary
my $primary_flush_lsn = $primary->safe_psql('postgres',
	"SELECT confirmed_flush_lsn from pg_replication_slots WHERE slot_name = 'lsub1_slot';");

# Confirm that restart_lsn and of confirmed_flush_lsn lsub1_slot slot are synced
# to the standby
ok( $standby1->poll_query_until(
		'postgres',
		"SELECT '$primary_restart_lsn' = restart_lsn AND '$primary_flush_lsn' = confirmed_flush_lsn from pg_replication_slots WHERE slot_name = 'lsub1_slot';"),
	'restart_lsn and confirmed_flush_lsn of slot lsub1_slot synced to standby');

##################################################
# Test that a synchronized slot can not be decoded, altered or dropped by the user
##################################################

# Disable hot_standby_feedback temporarily to stop slot sync worker otherwise
# the concerned testing scenarios here may be interrupted by different error:
# 'ERROR:  replication slot is active for PID ..'
$standby1->safe_psql('postgres', 'ALTER SYSTEM SET hot_standby_feedback = off;');
$standby1->restart;

# Attempting to perform logical decoding on a synced slot should result in an error
($result, $stdout, $stderr) = $standby1->psql('postgres',
	"select * from pg_logical_slot_get_changes('lsub1_slot',NULL,NULL);");
ok($stderr =~ /ERROR:  cannot use replication slot "lsub1_slot" for logical decoding/,
	"logical decoding is not allowed on synced slot");

# Attempting to alter a synced slot should result in an error
($result, $stdout, $stderr) = $standby1->psql(
    'postgres',
    qq[ALTER_REPLICATION_SLOT lsub1_slot (failover);],
    replication => 'database');
ok($stderr =~ /ERROR:  cannot alter replication slot "lsub1_slot"/,
	"synced slot on standby cannot be altered");

# Attempting to drop a synced slot should result in an error
($result, $stdout, $stderr) = $standby1->psql('postgres',
	"SELECT pg_drop_replication_slot('lsub1_slot');");
ok($stderr =~ /ERROR:  cannot drop replication slot "lsub1_slot"/,
	"synced slot on standby cannot be dropped");

# Enable hot_standby_feedback and restart standby
$standby1->safe_psql('postgres', 'ALTER SYSTEM SET hot_standby_feedback = on;');
$standby1->restart;

##################################################
# Promote the standby1 to primary. Confirm that:
# a) the slot 'lsub1_slot' is retained on the new primary
# b) logical replication for regress_mysub1 is resumed successfully after failover
##################################################
$standby1->promote;

# Update subscription with the new primary's connection info
$subscriber1->safe_psql('postgres',
	"ALTER SUBSCRIPTION regress_mysub1 CONNECTION '$standby1_conninfo';
	 ALTER SUBSCRIPTION regress_mysub1 ENABLE; ");

# Confirm the synced slot 'lsub1_slot' is retained on the new primary
is($standby1->safe_psql('postgres',
	q{SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'lsub1_slot';}),
	'lsub1_slot',
	'synced slot retained on the new primary');

# Insert data on the new primary
$standby1->safe_psql('postgres',
	"INSERT INTO tab_int SELECT generate_series(11, 20);");
$standby1->wait_for_catchup('regress_mysub1');

# Confirm that data in tab_int replicated on the subscriber
is( $subscriber1->safe_psql('postgres', q{SELECT count(*) FROM tab_int;}),
	"20",
	'data replicated from the new primary');

done_testing();
