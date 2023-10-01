
# Copyright (c) 2023, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Test primary disallowing specified logical replication slots getting ahead of
# specified physical replication slots. It uses the following set up:
#
#           	| ----> standby1 (connected via streaming replication)
#				| ----> standby2 (connected via streaming replication)
# primary -----	|
#		    	| ----> subscriber1 (connected via logical replication)
#		    	| ----> subscriber2 (connected via logical replication)
#
# Set up is configured in such a way that primary never lets subscriber1 ahead
# of standby1.

# Create primary
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 'logical');

# Configure primary to disallow specified logical replication slot (lsub1_slot)
# getting ahead of specified physical replication slot (sb1_slot).
$primary->append_conf(
	'postgresql.conf', qq(
standby_slot_names = 'sb1_slot'
synchronize_slot_names = 'lsub1_slot'
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
$publisher->safe_psql('postgres', "CREATE PUBLICATION mypub FOR TABLE tab_int;");
my $publisher_connstr = $publisher->connstr . ' dbname=postgres';

# Create a subscriber node, wait for sync to complete
my $subscriber1 = PostgreSQL::Test::Cluster->new('subscriber1');
$subscriber1->init(allows_streaming => 'logical');
$subscriber1->start;
$subscriber1->safe_psql('postgres', "CREATE TABLE tab_int (a int PRIMARY KEY);");
$subscriber1->safe_psql('postgres',
		"CREATE SUBSCRIPTION mysub1 CONNECTION '$publisher_connstr' "
	  . "PUBLICATION mypub WITH (slot_name = lsub1_slot);");
$subscriber1->wait_for_subscription_sync;

# Create another subscriber node, wait for sync to complete
my $subscriber2 = PostgreSQL::Test::Cluster->new('subscriber2');
$subscriber2->init(allows_streaming => 'logical');
$subscriber2->start;
$subscriber2->safe_psql('postgres', "CREATE TABLE tab_int (a int PRIMARY KEY);");
$subscriber2->safe_psql('postgres',
		"CREATE SUBSCRIPTION mysub2 CONNECTION '$publisher_connstr' "
	  . "PUBLICATION mypub WITH (slot_name = lsub2_slot);");
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

# Wait for the subscriber that's up and running and not specified in
# synchronize_slot_names GUC on primary gets the data from primary without
# waiting for any standbys.
$publisher->wait_for_catchup('mysub2');
$result = $subscriber2->safe_psql('postgres',
	"SELECT count(*) = $primary_row_count FROM tab_int;");
is($result, 't', "subscriber2 gets data from primary");

# The subscriber that's up and running and specified in synchronize_slot_names
# GUC on primary doesn't get the data from primary and keeps waiting for the
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
# primary must send the decoded changes to subscriber specified in
# synchronize_slot_names. While the standby was down, this subscriber didn't
# receive any data from primary i.e. the primary didn't allow it to go ahead
# of standby.
$publisher->wait_for_catchup('mysub1');
$result = $subscriber1->safe_psql('postgres',
	"SELECT count(*) = $primary_row_count FROM tab_int;");
is($result, 't', "subscriber1 gets data from primary after standby1 acknowledges changes");

done_testing();
