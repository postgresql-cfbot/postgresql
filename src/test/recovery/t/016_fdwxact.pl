# Tests for transaction involving foreign servers
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 7;

# Setup master node
my $node_master = get_new_node("master");
my $node_standby = get_new_node("standby");

$node_master->init(allows_streaming => 1);
$node_master->append_conf('postgresql.conf', qq(
max_prepared_transactions = 10
max_prepared_foreign_transactions = 10
max_foreign_transaction_resolvers = 2
foreign_transaction_resolver_timeout = 0
foreign_transaction_resolution_retry_interval = 5s
foreign_twophase_commit = on
));
$node_master->start;

# Take backup from master node
my $backup_name = 'master_backup';
$node_master->backup($backup_name);

# Set up standby node
$node_standby->init_from_backup($node_master, $backup_name,
							   has_streaming => 1);
$node_standby->start;

# Set up foreign nodes
my $node_fs1 = get_new_node("fs1");
my $node_fs2 = get_new_node("fs2");
my $fs1_port = $node_fs1->port;
my $fs2_port = $node_fs2->port;
$node_fs1->init;
$node_fs2->init;
$node_fs1->append_conf('postgresql.conf', qq(max_prepared_transactions = 10));
$node_fs2->append_conf('postgresql.conf', qq(max_prepared_transactions = 10));
$node_fs1->start;
$node_fs2->start;

# Create foreign servers on the master node
$node_master->safe_psql('postgres', qq(
CREATE EXTENSION postgres_fdw
));
$node_master->safe_psql('postgres', qq(
CREATE SERVER fs1 FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (dbname 'postgres', port '$fs1_port', two_phase_commit 'on');
));
$node_master->safe_psql('postgres', qq(
CREATE SERVER fs2 FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (dbname 'postgres', port '$fs2_port', two_phase_commit 'on');
));

# Create user mapping on the master node
$node_master->safe_psql('postgres', qq(
CREATE USER MAPPING FOR CURRENT_USER SERVER fs1;
CREATE USER MAPPING FOR CURRENT_USER SERVER fs2;
));

# Create tables on foreign nodes and import them to the master node
$node_fs1->safe_psql('postgres', qq(
CREATE SCHEMA fs;
CREATE TABLE fs.t1 (c int);
));
$node_fs2->safe_psql('postgres', qq(
CREATE SCHEMA fs;
CREATE TABLE fs.t2 (c int);
));
$node_master->safe_psql('postgres', qq(
IMPORT FOREIGN SCHEMA fs FROM SERVER fs1 INTO public;
IMPORT FOREIGN SCHEMA fs FROM SERVER fs2 INTO public;
CREATE TABLE l_table (c int);
));

# Switch to synchronous replication
$node_master->safe_psql('postgres', qq(
ALTER SYSTEM SET synchronous_standby_names ='*';
));
$node_master->reload;

my $result;

# Prepare two transactions involving multiple foreign servers and shutdown
# the master node. Check if we can commit and rollback the foreign transactions
# after the normal recovery.
$node_master->safe_psql('postgres', qq(
BEGIN;
INSERT INTO t1 VALUES (1);
INSERT INTO t2 VALUES (1);
PREPARE TRANSACTION 'gxid1';
BEGIN;
INSERT INTO t1 VALUES (2);
INSERT INTO t2 VALUES (2);
PREPARE TRANSACTION 'gxid2';
));

$node_master->stop;
$node_master->start;

# Commit and rollback foreign transactions after the recovery.
$result = $node_master->psql('postgres', qq(COMMIT PREPARED 'gxid1'));
is($result, 0, 'Commit foreign transactions after recovery');
$result = $node_master->psql('postgres', qq(ROLLBACK PREPARED 'gxid2'));
is($result, 0, 'Rollback foreign transactions after recovery');

#
# Prepare two transactions involving multiple foreign servers and shutdown
# the master node immediately. Check if we can commit and rollback the foreign
# transactions after the crash recovery.
#
$node_master->safe_psql('postgres', qq(
BEGIN;
INSERT INTO t1 VALUES (3);
INSERT INTO t2 VALUES (3);
PREPARE TRANSACTION 'gxid1';
BEGIN;
INSERT INTO t1 VALUES (4);
INSERT INTO t2 VALUES (4);
PREPARE TRANSACTION 'gxid2';
));

$node_master->teardown_node;
$node_master->start;

# Commit and rollback foreign transactions after the crash recovery.
$result = $node_master->psql('postgres', qq(COMMIT PREPARED 'gxid1'));
is($result, 0, 'Commit foreign transactions after crash recovery');
$result = $node_master->psql('postgres', qq(ROLLBACK PREPARED 'gxid2'));
is($result, 0, 'Rollback foreign transactions after crash recovery');

#
# Commit transaction involving foreign servers and shutdown the master node
# immediately before checkpoint. Check that WAL replay cleans up
# its shared memory state release locks while replaying transaction commit.
#
$node_master->safe_psql('postgres', qq(
BEGIN;
INSERT INTO t1 VALUES (5);
INSERT INTO t2 VALUES (5);
COMMIT;
));

$node_master->teardown_node;
$node_master->start;

$result = $node_master->safe_psql('postgres', qq(
SELECT count(*) FROM pg_prepared_fdw_xacts;
));
is($result, 0, "Cleanup of shared memory state for foreign transactions");

#
# Check if the standby node can process prepared foreign transaction
# after promotion.
#
$node_master->safe_psql('postgres', qq(
BEGIN;
INSERT INTO t1 VALUES (6);
INSERT INTO t2 VALUES (6);
PREPARE TRANSACTION 'gxid1';
BEGIN;
INSERT INTO t1 VALUES (7);
INSERT INTO t2 VALUES (7);
PREPARE TRANSACTION 'gxid2';
));

$node_master->teardown_node;
$node_standby->promote;

$result = $node_standby->psql('postgres', qq(COMMIT PREPARED 'gxid1';));
is($result, 0, 'Commit foreign transaction after promotion');
$result = $node_standby->psql('postgres', qq(ROLLBACK PREPARED 'gxid2';));
is($result, 0, 'Rollback foreign transaction after promotion');
