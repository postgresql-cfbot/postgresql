
# Copyright (c) 2024, PostgreSQL Global Development Group

# Test the conflict detection and resolution in logical replication
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

###############################
# Setup
###############################

# Initialize publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	qq(max_prepared_transactions = 10));
$node_publisher->start;

# Create subscriber node with track_commit_timestamp enabled
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->append_conf('postgresql.conf',
	qq(track_commit_timestamp = on));
$node_subscriber->start;

# Create table on publisher
$node_publisher->safe_psql(
	'postgres',
	"CREATE TABLE conf_tab(a int PRIMARY key, data text, comments text);
	 ALTER TABLE conf_tab ALTER COLUMN comments SET STORAGE EXTERNAL;");

# Create similar table on subscriber
$node_subscriber->safe_psql(
	'postgres',
	"CREATE TABLE conf_tab(a int PRIMARY key, data text, comments text);
	 ALTER TABLE conf_tab ALTER COLUMN comments SET STORAGE EXTERNAL;");

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE conf_tab");

# Create the subscription
my $appname = 'tap_sub';
$node_subscriber->safe_psql(
	'postgres',
	"CREATE SUBSCRIPTION tap_sub
	CONNECTION '$publisher_connstr application_name=$appname'
	PUBLICATION tap_pub;");

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

my $result = $node_subscriber->safe_psql('postgres',
	"SELECT conftype, confres FROM pg_subscription_conflict ORDER BY conftype"
);
is( $result, qq(delete_missing|skip
delete_origin_differs|apply_remote
insert_exists|error
update_exists|error
update_missing|skip
update_origin_differs|apply_remote),
	"confirm that the default conflict resolvers are in place");

############################################
# Test 'apply_remote' for 'insert_exists'
############################################

# Change CONFLICT RESOLVER of insert_exists to apply_remote
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (insert_exists = 'apply_remote');"
);

# Create local data on the subscriber
$node_subscriber->safe_psql('postgres',
	"INSERT INTO conf_tab(a, data) VALUES (1,'fromsub')");

# Create conflicting data on the publisher
my $log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"INSERT INTO conf_tab(a, data) VALUES (1,'frompub')");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab\": conflict=insert_exists, resolution=apply_remote/,
	$log_offset);

# Confirm that remote insert is converted to an update and the remote data is updated.
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data FROM conf_tab WHERE (a=1);");

is($result, 'frompub', "remote data is kept");


########################################
# Test 'keep_local' for 'insert_exists'
########################################

# Change CONFLICT RESOLVER of insert_exists to keep_local
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (insert_exists = 'keep_local');"
);

# Create local data on the subscriber
$node_subscriber->safe_psql('postgres',
	"INSERT INTO conf_tab(a, data) VALUES (2,'fromsub')");

# Confirm that row is updated
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data FROM conf_tab WHERE (a=2);");

is($result, 'fromsub', "data 2 from local is inserted");

$log_offset = -s $node_subscriber->logfile;

# Create conflicting data on the publisher
$node_publisher->safe_psql('postgres',
	"INSERT INTO conf_tab(a, data) VALUES (2,'frompub')");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab\": conflict=insert_exists, resolution=keep_local/,
	$log_offset);

# Confirm that remote insert is ignored and the local row is kept
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data FROM conf_tab WHERE (a=2);");

is($result, 'fromsub', "data from local is kept");

###################################
# Test 'error' for 'insert_exists'
###################################

# Change CONFLICT RESOLVER of insert_exists to error
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (insert_exists = 'error');"
);

# Create local data on the subscriber
$node_subscriber->safe_psql('postgres',
	"INSERT INTO conf_tab(a, data) VALUES (3,'fromsub')");

# Create conflicting data on the publisher
$node_publisher->safe_psql('postgres',
	"INSERT INTO conf_tab(a, data) VALUES (3,'frompub')");

$log_offset = -s $node_subscriber->logfile;

# Confirm that this causes an error on the subscriber
$node_subscriber->wait_for_log(
	qr/ERROR:  conflict detected on relation \"public.conf_tab\": conflict=insert_exists, resolution=error/,
	$log_offset);

# Truncate table on subscriber to get rid of the error
$node_subscriber->safe_psql('postgres', "TRUNCATE conf_tab;");

# Truncate the table on the publisher
$node_publisher->safe_psql('postgres', "TRUNCATE conf_tab;");

# Drop the subscriber to remove error
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub;");

# Create the subscription
$node_subscriber->safe_psql(
	'postgres',
	"CREATE SUBSCRIPTION tap_sub
	 CONNECTION '$publisher_connstr application_name=$appname'
	 PUBLICATION tap_pub");

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);


############################################
# Test 'apply_remote' for 'update_exists'
############################################
# Change CONFLICT RESOLVER of update_exists to apply_remote
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (update_exists = 'apply_remote');"
);

# Insert data in the subscriber
$node_subscriber->safe_psql(
	'postgres',
	"INSERT INTO conf_tab(a, data) VALUES (1,'fromsub');
	 INSERT INTO conf_tab(a, data) VALUES (2,'fromsub');
	 INSERT INTO conf_tab(a, data) VALUES (3,'fromsub');");

# Insert data in the publisher
$node_publisher->safe_psql(
	'postgres',
	"INSERT INTO conf_tab(a, data) VALUES (4,'frompub');
	 INSERT INTO conf_tab(a, data) VALUES (5,'frompub');
	 INSERT INTO conf_tab(a, data) VALUES (6,'frompub');");

$log_offset = -s $node_subscriber->logfile;

# Update on publisher which already exists on subscriber
$node_publisher->safe_psql('postgres', "UPDATE conf_tab SET a=1 WHERE a=4;");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab\": conflict=update_exists, resolution=apply_remote/,
	$log_offset);

# Confirm that remote update is applied.
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data FROM conf_tab WHERE (a=1);");

is($result, 'frompub', "remote data is kept");

########################################
# Test 'keep_local' for 'update_exists'
########################################

# Change CONFLICT RESOLVER of update_exists to keep_local
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (update_exists = 'keep_local');"
);

$log_offset = -s $node_subscriber->logfile;

# Update on publisher which already exists on subscriber
$node_publisher->safe_psql('postgres', "UPDATE conf_tab SET a=2 WHERE a=5;");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab\": conflict=update_exists, resolution=keep_local/,
	$log_offset);

# Confirm that remote insert is ignored and the local row is kept
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data FROM conf_tab WHERE (a=2);");

is($result, 'fromsub', "data from local is kept");

###################################
# Test 'error' for 'update_exists'
###################################

# Change CONFLICT RESOLVER of update_exists to error
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (update_exists = 'error');"
);

# Update on publisher which already exists on subscriber
$node_publisher->safe_psql('postgres', "UPDATE conf_tab SET a=3 WHERE a=6;");

$log_offset = -s $node_subscriber->logfile;

# Confirm that this causes an error on the subscriber
$node_subscriber->wait_for_log(
	qr/ERROR:  conflict detected on relation \"public.conf_tab\": conflict=update_exists, resolution=error/,
	$log_offset);

# Truncate table on subscriber to get rid of the error
$node_subscriber->safe_psql('postgres', "TRUNCATE conf_tab;");

###################################
# Test 'skip' for 'delete_missing'
###################################

# Delete row on publisher that is not present on the subscriber and confirm that it is skipped
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres', "DELETE FROM conf_tab WHERE (a=2);");

# Confirm that the missing row is skipped because 'delete_missing' is set to 'skip'
$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab\": conflict=delete_missing, resolution=skip/,
	$log_offset);

####################################
# Test 'error' for 'delete_missing'
####################################

# Change CONFLICT RESOLVER of delete_missing to error
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (delete_missing = 'error');"
);

# Capture the log offset before performing the delete on the publisher
$log_offset = -s $node_subscriber->logfile;

# Perform the delete on the publisher
$node_publisher->safe_psql('postgres', "DELETE FROM conf_tab WHERE (a=1);");

# Confirm that this causes an error on the subscriber
$node_subscriber->wait_for_log(
	qr/ERROR:  conflict detected on relation \"public.conf_tab\": conflict=delete_missing, resolution=error/,
	$log_offset);

# Drop the subscriber to remove error
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub;");

# Truncate table on subscriber
$node_subscriber->safe_psql('postgres', "TRUNCATE conf_tab;");

# Truncate the table on the publisher
$node_publisher->safe_psql('postgres', "TRUNCATE conf_tab;");

# Create the subscription
$node_subscriber->safe_psql(
	'postgres',
	"CREATE SUBSCRIPTION tap_sub
	 CONNECTION '$publisher_connstr application_name=$appname'
	 PUBLICATION tap_pub");

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);


#################################################
# Test 'apply_remote' for 'delete_origin_differs'
#################################################

# Insert data in the publisher
$node_publisher->safe_psql(
	'postgres',
	"INSERT INTO conf_tab(a, data) VALUES (1,'frompub');
	 INSERT INTO conf_tab(a, data) VALUES (2,'frompub');
	 INSERT INTO conf_tab(a, data) VALUES (3,'frompub');");

# Modify data on the subscriber
$node_subscriber->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'fromsub' WHERE (a=1);");

# Create a conflicting delete on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres', "DELETE FROM conf_tab WHERE (a=1);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab\": conflict=delete_origin_differs, resolution=apply_remote/,
	$log_offset);

# Confirm that the remote delete the local updated row
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data from conf_tab WHERE (a=1);");

is($result, '', "delete from remote is applied");

###############################################
# Test 'keep_local' for 'delete_origin_differs'
###############################################

# Change CONFLICT RESOLVER of delete_origin_differs to keep_local
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (delete_origin_differs = 'keep_local');"
);

# Modify data on the subscriber
$node_subscriber->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'fromsub' WHERE (a=2);");

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres', "DELETE FROM conf_tab WHERE (a=2);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab\": conflict=delete_origin_differs, resolution=keep_local/,
	$log_offset);

# Confirm that the local data is untouched by the remote update
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data from conf_tab WHERE (a=2);");

is($result, 'fromsub', "update from local is kept");

##########################################
# Test 'error' for 'delete_origin_differs'
##########################################

# Change CONFLICT RESOLVER of delete_origin_differs to error
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (delete_origin_differs = 'error');"
);

# Modify data on the subscriber
$node_subscriber->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'fromsub' WHERE (a=3);");

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres', "DELETE FROM conf_tab WHERE (a=3);");

$node_subscriber->wait_for_log(
	qr/ERROR:  conflict detected on relation \"public.conf_tab\": conflict=delete_origin_differs, resolution=error/,
	$log_offset);

# Drop the subscriber to remove error
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub;");

# Truncate the table on the publisher
$node_publisher->safe_psql('postgres', "TRUNCATE conf_tab;");

# Truncate the table on the subscriber
$node_subscriber->safe_psql('postgres', "TRUNCATE conf_tab;");

# Create the subscription
$node_subscriber->safe_psql(
	'postgres',
	"CREATE SUBSCRIPTION tap_sub
	 CONNECTION '$publisher_connstr application_name=$appname'
	 PUBLICATION tap_pub");

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

#################################################
# Test 'apply_remote' for 'update_origin_differs'
#################################################

# Insert data in the publisher
$node_publisher->safe_psql(
	'postgres',
	"INSERT INTO conf_tab(a, data) VALUES (1,'frompub');
	 INSERT INTO conf_tab(a, data) VALUES (2,'frompub');
	 INSERT INTO conf_tab(a, data) VALUES (3,'frompub');");

# Modify data on the subscriber
$node_subscriber->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'fromsub' WHERE (a=1);");

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'frompubnew' WHERE (a=1);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab\": conflict=update_origin_differs, resolution=apply_remote/,
	$log_offset);

# Confirm that the remote update overrides the local update
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data from conf_tab WHERE (a=1);");

is($result, 'frompubnew', "update from remote is kept");

###############################################
# Test 'keep_local' for 'update_origin_differs'
###############################################

# Change CONFLICT RESOLVER of update_origin_differs to keep_local
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (update_origin_differs = 'keep_local');"
);

# Modify data on the subscriber
$node_subscriber->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'fromsub' WHERE (a=2);");

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'frompubnew' WHERE (a=2);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab\": conflict=update_origin_differs, resolution=keep_local/,
	$log_offset);

# Confirm that the local data is untouched by the remote update
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data from conf_tab WHERE (a=2);");

is($result, 'fromsub', "update from local is kept");

##########################################
# Test 'error' for 'update_origin_differs'
##########################################

# Change CONFLICT RESOLVER of update_origin_differs to error
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (update_origin_differs = 'error');"
);

# Modify data on the subscriber
$node_subscriber->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'fromsub' WHERE (a=3);");

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'frompubnew' WHERE (a=3);");

$node_subscriber->wait_for_log(
	qr/ERROR:  conflict detected on relation \"public.conf_tab\": conflict=update_origin_differs, resolution=error/,
	$log_offset);

# Drop the subscriber to remove error
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub;");

# Truncate the table on the publisher
$node_publisher->safe_psql('postgres', "TRUNCATE conf_tab;");

# Truncate the table on the subscriber
$node_subscriber->safe_psql('postgres', "TRUNCATE conf_tab;");

# Create the subscription
$node_subscriber->safe_psql(
	'postgres',
	"CREATE SUBSCRIPTION tap_sub
	 CONNECTION '$publisher_connstr application_name=$appname'
	 PUBLICATION tap_pub");

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

# Insert data in the publisher
$node_publisher->safe_psql(
	'postgres',
	"INSERT INTO conf_tab(a, data) VALUES (1,'frompub');
	 INSERT INTO conf_tab(a, data) VALUES (2,'frompub');
	 INSERT INTO conf_tab(a, data) VALUES (3,'frompub');");

###########################################
# Test 'apply_or_skip' for 'update_missing'
###########################################

# Change CONFLICT RESOLVER of update_missing to apply_or_skip
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (update_missing = 'apply_or_skip');"
);

# Test the apply part

# Delete the row on the subscriber
$node_subscriber->safe_psql('postgres', "DELETE FROM conf_tab WHERE (a=3);");

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'frompubnew' WHERE (a=3);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab\": conflict=update_missing, resolution=apply_or_skip/,
	$log_offset);

# Confirm that the remote update is converted to an insert and new row applied
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data from conf_tab WHERE (a=3);");

is($result, 'frompubnew', "update from remote is converted to insert");

# Test the skip part

# Create new row on publisher with toast data
$node_publisher->safe_psql('postgres',
	"INSERT INTO conf_tab (a, data, comments) VALUES(4,'frompub',repeat('abcdefghij', 200));"
);

# Delete the row on the subscriber
$node_subscriber->safe_psql('postgres', "DELETE FROM conf_tab WHERE (a=4);");

# Update the row on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'frompubnew' WHERE (a=4);");

$node_subscriber->wait_for_log(
	qr/DETAIL:  Could not find the row to be updated, and the UPDATE cannot be converted to an INSERT, thus skipping the remote changes./,
	$log_offset);

###########################################
# Test 'apply_or_error' for 'update_missing'
###########################################

# Test the apply part

# Change CONFLICT RESOLVER of update_missing to apply_or_error
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (update_missing = 'apply_or_error');"
);

# Create new row on the publisher
$node_publisher->safe_psql('postgres',
	"INSERT INTO conf_tab(a, data) VALUES (5,'frompub');");

# Delete the row on the subscriber
$node_subscriber->safe_psql('postgres', "DELETE FROM conf_tab WHERE (a=5);");

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'frompubnew' WHERE (a=5);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab\": conflict=update_missing, resolution=apply_or_error/,
	$log_offset);

# Confirm that the remote update is converted to an insert and new row applied
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data from conf_tab WHERE (a=5);");

is($result, 'frompubnew', "update from remote is converted to insert");

# Test the error part

# Create new row on publisher with toast data
$node_publisher->safe_psql('postgres',
	"INSERT INTO conf_tab (a, data, comments) VALUES(6,'frompub',repeat('abcdefghij', 200));"
);

# Delete the row on the subscriber
$node_subscriber->safe_psql('postgres', "DELETE FROM conf_tab WHERE (a=6);");

# Update the row on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'frompubnew' WHERE (a=6);");

$node_subscriber->wait_for_log(
	qr/ERROR:  conflict detected on relation \"public.conf_tab\": conflict=update_missing, resolution=apply_or_error/,
	$log_offset);

# Drop the subscriber to remove error
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub;");

# Truncate the table on the publisher
$node_publisher->safe_psql('postgres', "TRUNCATE conf_tab;");

# Truncate the table on the subscriber
$node_subscriber->safe_psql('postgres', "TRUNCATE conf_tab;");

# Create the subscription
$node_subscriber->safe_psql(
	'postgres',
	"CREATE SUBSCRIPTION tap_sub
	 CONNECTION '$publisher_connstr application_name=$appname'
	 PUBLICATION tap_pub");

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

# Insert data in the publisher
$node_publisher->safe_psql(
	'postgres',
	"INSERT INTO conf_tab(a, data) VALUES (1,'frompub');
	 INSERT INTO conf_tab(a, data) VALUES (2,'frompub');
	 INSERT INTO conf_tab(a, data) VALUES (3,'frompub');");

###################################
# Test 'skip' for 'update_missing'
###################################

# Change CONFLICT RESOLVER of update_missing to skip
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (update_missing = 'skip');"
);

# Delete the row on the subscriber
$node_subscriber->safe_psql('postgres', "DELETE FROM conf_tab WHERE (a=2);");

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'frompubnew' WHERE (a=2);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab\": conflict=update_missing, resolution=skip/,
	$log_offset);

# Confirm that the update does not change anything on the subscriber
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data from conf_tab WHERE (a=2);");

is($result, '', "update from remote is skipped on the subscriber");

###################################
# Test 'error' for 'update_missing'
###################################

# Change CONFLICT RESOLVER of update_missing to error
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONFLICT RESOLVER (update_missing = 'error');"
);

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab SET data = 'frompubnewer' WHERE (a=2);");

$node_subscriber->wait_for_log(
	qr/ERROR:  conflict detected on relation \"public.conf_tab\": conflict=update_missing, resolution=error/,
	$log_offset);

# Drop the subscriber to remove error
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub;");

#################################################
# Partition table tests for UPDATE conflicts
#################################################

# Create partitioned table on publisher
$node_publisher->safe_psql(
	'postgres',
	"CREATE TABLE conf_tab_part (a int not null, b int not null, data text);
	 ALTER TABLE conf_tab_part ADD CONSTRAINT conf_tab_part_pk primary key (a,b);"
);

# Create similar table on subscriber but with partitions
$node_subscriber->safe_psql(
	'postgres',
	"CREATE TABLE conf_tab_part (a int not null, b int not null, data text) partition by range (b);
	 ALTER TABLE conf_tab_part ADD CONSTRAINT conf_tab_part_pk primary key (a,b);
	 CREATE TABLE conf_tab_part_1 PARTITION OF conf_tab_part FOR VALUES FROM (MINVALUE) TO (100);
	 CREATE TABLE conf_tab_part_2 PARTITION OF conf_tab_part FOR VALUES FROM (101) TO (MAXVALUE);"
);

# Setup logical replication
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub_part FOR TABLE conf_tab_part with (publish_via_partition_root=true);"
);

# Create the subscription
$appname = 'sub_part';
$node_subscriber->safe_psql(
	'postgres',
	"CREATE SUBSCRIPTION sub_part
	CONNECTION '$publisher_connstr application_name=$appname'
	PUBLICATION pub_part;");

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

#################################################
# Test 'apply_remote' for 'update_origin_differs'
#################################################

# Insert data in the publisher
$node_publisher->safe_psql(
	'postgres',
	"INSERT INTO conf_tab_part VALUES (1,1,'frompub');
	 INSERT INTO conf_tab_part VALUES (2,1,'frompub');
	 INSERT INTO conf_tab_part VALUES (3,1,'frompub');");

# Modify data on the subscriber
$node_subscriber->safe_psql('postgres',
	"UPDATE conf_tab_part SET data = 'fromsub' WHERE (a=1);");

# Create a conflicting update on the same partition
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab_part SET data = 'frompubnew_p1' WHERE (a=1);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab_part_1\": conflict=update_origin_differs, resolution=apply_remote/,
	$log_offset);

# Confirm that the remote update overrides the local update
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data from conf_tab_part WHERE (a=1);");

is($result, 'frompubnew_p1',
	"update from remote is applied to the first partition");

# Create a conflicting update which also changes the partition
$node_subscriber->safe_psql('postgres',
	"UPDATE conf_tab_part SET data = 'fromsub' WHERE (a=1);");
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab_part SET b=101, data='frompubnew_p2' WHERE (a=1);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab_part_1\": conflict=update_origin_differs, resolution=apply_remote/,
	$log_offset);

# Confirm that the remote update overrides the local update
$result = $node_subscriber->safe_psql('postgres',
	"SELECT b, data from conf_tab_part WHERE (a=1);");

is($result, qq(101|frompubnew_p2),
	"update from remote is the second partition");

###############################################
# Test 'keep_local' for 'update_origin_differs'
###############################################

# Change CONFLICT RESOLVER of update_origin_differs to keep_local
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub_part CONFLICT RESOLVER (update_origin_differs = 'keep_local');"
);

# Modify data on the subscriber
$node_subscriber->safe_psql('postgres',
	"UPDATE conf_tab_part SET data = 'fromsub' WHERE (a=2);");

# Create a conflicting update on the same partition
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab_part SET data = 'frompubnew_p1' WHERE (a=2);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab_part_1\": conflict=update_origin_differs, resolution=keep_local/,
	$log_offset);

# Confirm that the local data is untouched by the remote update
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data from conf_tab_part WHERE (a=2);");

is($result, 'fromsub', "update from local is kept on the first partition");

# Create a conflicting update which also changes the partition
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab_part SET b=102, data='frompubnew_p2' WHERE (a=2);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab_part_1\": conflict=update_origin_differs, resolution=keep_local/,
	$log_offset);

# Confirm that the local data is untouched by the remote update
$result = $node_subscriber->safe_psql('postgres',
	"SELECT b, data from conf_tab_part WHERE (a=2);");

is($result, qq(1|fromsub),
	"update from local is kept on the first partition");

##########################################
# Test 'error' for 'update_origin_differs'
##########################################

# Change CONFLICT RESOLVER of update_origin_differs to error
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub_part CONFLICT RESOLVER (update_origin_differs = 'error');"
);

# Modify data on the subscriber
$node_subscriber->safe_psql('postgres',
	"UPDATE conf_tab_part SET data = 'fromsub' WHERE (a=3);");

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab_part SET data = 'frompubnew' WHERE (a=3);");

$node_subscriber->wait_for_log(
	qr/ERROR:  conflict detected on relation \"public.conf_tab_part_1\": conflict=update_origin_differs, resolution=error/,
	$log_offset);

# Drop the subscriber to remove error
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION sub_part;");

# Truncate the table on the publisher
$node_publisher->safe_psql('postgres', "TRUNCATE conf_tab_part;");

# Truncate the table on the subscriber
$node_subscriber->safe_psql('postgres', "TRUNCATE conf_tab_part;");

$node_subscriber->safe_psql(
	'postgres',
	"CREATE SUBSCRIPTION sub_part
	CONNECTION '$publisher_connstr application_name=$appname'
	PUBLICATION pub_part;");

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

# Insert data in the publisher
$node_publisher->safe_psql(
	'postgres',
	"INSERT INTO conf_tab_part VALUES (1,1,'frompub');
	 INSERT INTO conf_tab_part VALUES (2,1,'frompub');
	 INSERT INTO conf_tab_part VALUES (3,1,'frompub');");

###########################################
# Test 'apply_or_skip' for 'update_missing'
###########################################

# Change CONFLICT RESOLVER of update_missing to apply_or_skip
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub_part CONFLICT RESOLVER (update_missing = 'apply_or_skip');"
);

# Delete the row on the subscriber
$node_subscriber->safe_psql('postgres',
	"DELETE FROM conf_tab_part WHERE (a=3);");

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab_part SET data='frompubnew_p1' WHERE (a=3);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab_part_1\": conflict=update_missing, resolution=apply_or_skip/,
	$log_offset);

# Confirm that the remote update is converted to an insert and new row applied
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data from conf_tab_part WHERE (a=3);");

is($result, 'frompubnew_p1',
	"update from remote is converted to insert in the first partition");

# Test the update which also changes the partition
# Delete the row on the subscriber
$node_subscriber->safe_psql('postgres',
	"DELETE FROM conf_tab_part WHERE (a=3);");

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab_part SET b=103, data='frompubnew_p2' WHERE (a=3);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab_part_1\": conflict=update_missing, resolution=apply_or_skip/,
	$log_offset);

# Confirm that the remote update is converted to an insert and new row applied
$result = $node_subscriber->safe_psql('postgres',
	"SELECT b,data from conf_tab_part WHERE (a=3);");

is($result, '103|frompubnew_p2',
	"update from remote is converted to insert in the second partition");

###################################
# Test 'skip' for 'update_missing'
###################################

# Change CONFLICT RESOLVER of update_missing to skip
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub_part CONFLICT RESOLVER (update_missing = 'skip');"
);

# Delete the row on the subscriber
$node_subscriber->safe_psql('postgres',
	"DELETE FROM conf_tab_part WHERE (a=2);");

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab_part SET data='frompubnew_p1' WHERE (a=2);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab_part_1\": conflict=update_missing, resolution=skip/,
	$log_offset);

# Confirm that the update does not change anything on the subscriber
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data from conf_tab_part WHERE (a=2);");

is($result, '',
	"update from remote on first partition is skipped on the subscriber");

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab_part SET b=102, data='frompubnew_p2' WHERE (a=2);");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab_part_1\": conflict=update_missing, resolution=skip/,
	$log_offset);

# Confirm that the update does not change anything on the subscriber
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data from conf_tab_part WHERE (a=2);");

is($result, '',
	"update from remote on second partition is skipped on the subscriber");

###################################
# Test 'error' for 'update_missing'
###################################

# Change CONFLICT RESOLVER of update_missing to error
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub_part CONFLICT RESOLVER (update_missing = 'error');"
);

# Create a conflicting update on the publisher
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab_part SET data = 'frompubnewer' WHERE (a=2);");

$node_subscriber->wait_for_log(
	qr/ERROR:  conflict detected on relation \"public.conf_tab_part_2\": conflict=update_missing, resolution=error/,
	$log_offset);

# Drop the subscriber to remove error
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION sub_part;");

# Truncate the table on the publisher
$node_publisher->safe_psql('postgres', "TRUNCATE conf_tab_part;");

# Truncate the table on the subscriber
$node_subscriber->safe_psql('postgres', "TRUNCATE conf_tab_part;");

$node_subscriber->safe_psql(
	'postgres',
	"CREATE SUBSCRIPTION sub_part
	CONNECTION '$publisher_connstr application_name=$appname'
	PUBLICATION pub_part;");

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

############################################
# Test 'apply_remote' for 'update_exists'
############################################
# Change CONFLICT RESOLVER of update_exists to apply_remote
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub_part CONFLICT RESOLVER (update_exists = 'apply_remote');"
);

# Insert data in the subscriber
$node_subscriber->safe_psql(
	'postgres',
	"INSERT INTO conf_tab_part VALUES (1,1,'fromsub');
	 INSERT INTO conf_tab_part VALUES (2,1,'fromsub');
	 INSERT INTO conf_tab_part VALUES (3,1,'fromsub');");

# Insert data in the publisher
$node_publisher->safe_psql(
	'postgres',
	"INSERT INTO conf_tab_part VALUES (4,1,'frompub');
	 INSERT INTO conf_tab_part VALUES (5,1,'frompub');
	 INSERT INTO conf_tab_part VALUES (6,1,'frompub');");

$log_offset = -s $node_subscriber->logfile;

# Update on publisher which already exists on subscriber
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab_part SET a=1 WHERE a=4;");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab_part_1\": conflict=update_exists, resolution=apply_remote/,
	$log_offset);

# Confirm that remote update is applied.
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data FROM conf_tab_part WHERE (a=1);");

is($result, 'frompub', "update from remote on partition is kept");

########################################
# Test 'keep_local' for 'update_exists'
########################################

# Change CONFLICT RESOLVER of update_exists to keep_local
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub_part CONFLICT RESOLVER (update_exists = 'keep_local');"
);

$log_offset = -s $node_subscriber->logfile;

# Update on publisher which already exists on subscriber
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab_part SET a=2 WHERE a=5;");

$node_subscriber->wait_for_log(
	qr/LOG:  conflict detected on relation \"public.conf_tab_part_1\": conflict=update_exists, resolution=keep_local/,
	$log_offset);

# Confirm that remote insert is ignored and the local row is kept
$result = $node_subscriber->safe_psql('postgres',
	"SELECT data FROM conf_tab_part WHERE (a=2);");

is($result, 'fromsub', "update from remote on partition is skipped");

###################################
# Test 'error' for 'update_exists'
###################################

# Change CONFLICT RESOLVER of update_exists to error
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub_part CONFLICT RESOLVER (update_exists = 'error');"
);

# Update on publisher which already exists on subscriber
$node_publisher->safe_psql('postgres',
	"UPDATE conf_tab_part SET a=3 WHERE a=6;");

$log_offset = -s $node_subscriber->logfile;

# Confirm that this causes an error on the subscriber
$node_subscriber->wait_for_log(
	qr/ERROR:  conflict detected on relation \"public.conf_tab_part_1\": conflict=update_exists, resolution=error/,
	$log_offset);

done_testing();
