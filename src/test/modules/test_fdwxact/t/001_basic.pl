use File::Copy qw/copy move/;
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 7;

my $node = get_new_node('main');
$node->init;
$node->append_conf('postgresql.conf', qq(
shared_preload_libraries = 'test_fdwxact'
max_prepared_transactions = 10
max_prepared_foreign_transactions = 10
max_foreign_transaction_resolvers = 2
foreign_transaction_resolver_timeout = 0
foreign_transaction_resolution_retry_interval = 5s
foreign_twophase_commit = required
test_fdwxact.log_api_calls = true
				   ));
$node->start;

$node->psql(
	'postgres', "
CREATE EXTENSION test_fdwxact;
CREATE SERVER srv FOREIGN DATA WRAPPER test_fdw;
CREATE SERVER srv_no2pc FOREIGN DATA WRAPPER test_no2pc_fdw;
CREATE SERVER srv_2pc_1 FOREIGN DATA WRAPPER test_2pc_fdw;
CREATE SERVER srv_2pc_2 FOREIGN DATA WRAPPER test_2pc_fdw;

CREATE TABLE t (i int);
CREATE FOREIGN TABLE ft (i int) SERVER srv;
CREATE FOREIGN TABLE ft_no2pc (i int) SERVER srv_no2pc;
CREATE FOREIGN TABLE ft_2pc_1 (i int) SERVER srv_2pc_1;
CREATE FOREIGN TABLE ft_2pc_2 (i int) SERVER srv_2pc_2;

CREATE USER MAPPING FOR PUBLIC SERVER srv;
CREATE USER MAPPING FOR PUBLIC SERVER srv_no2pc;
CREATE USER MAPPING FOR PUBLIC SERVER srv_2pc_1;
CREATE USER MAPPING FOR PUBLIC SERVER srv_2pc_2;
	");

sub run_transaction
{
	my ($node, $prepsql, $sql, $endsql, $expected) = @_;

	$endsql = 'COMMIT' unless defined $endsql;
	$expected = 0 unless defined $expected;

	local $ENV{PGHOST} = $node->host;
	local $ENV{PGPORT} = $node->port;

	truncate $node->logfile, 0;

	$node->safe_psql('postgres', $prepsql);
	my ($cmdret, $stdout, $stderr) = $node->psql('postgres',
												 "BEGIN;
												 SELECT txid_current() as xid;
												 $sql
												 $endsql;
												 ");
	$node->poll_query_until('postgres',
							"SELECT count(*) = $expected FROM pg_foreign_xacts");

	my $log = TestLib::slurp_file($node->logfile);

	return $log, $stdout;
}

my ($log, $xid);

# The transaction is committed using two-phase commit.
($log, $xid) = run_transaction($node, "",
							   "INSERT INTO ft_2pc_1 VALUES(1);
							   INSERT INTO ft_2pc_2 VALUES(1);");
like($log, qr/commit prepared tx_$xid on srv_2pc_1/, "commit prepared transaction-1");
like($log, qr/commit prepared tx_$xid on srv_2pc_2/, "commit prepared transaction-2");

# Similary, two-phase commit is used.
($log, $xid) = run_transaction($node, "",
					  "INSERT INTO t VALUES(1);
					  INSERT INTO ft_2pc_1 VALUES(1);");
like($log, qr/commit prepared tx_$xid on srv_2pc_1/, "commit prepared transaction-3");

# Test the failure case of PREPARE TRANSACTION. We prepare the distributed
# transaction with the same identifer.  The second attempt will fail when preparing
# the local transaction, which is performed after preparing the foreign transaction
# on srv_2pc_1. Therefore the transaction should rollback the prepared foreign
# transaction.
($log, $xid) = run_transaction($node, "",
							   "INSERT INTO t VALUES(1);
							   INSERT INTO ft_2pc_1 VALUES(1);",
							   "PREPARE TRANSACTION 'tx1'", 1);
($log, $xid) = run_transaction($node, "",
							   "INSERT INTO t VALUES(1);
							   INSERT INTO ft_2pc_1 VALUES(1);",
							   "PREPARE TRANSACTION 'tx1'", 1);
like($log, qr/rollback prepared tx_$xid on srv_2pc_1/, "failure after prepare transaction");
$node->safe_psql('postgres', "COMMIT PREPARED 'tx1'");

# Inject an error into prepare phase on srv_2pc_1. The transaction fails during
# preparing the foreign transaction on srv_2pc_1. Then, we try to both 'rollback' and
# 'rollback prepared' the foreign transaction, and rollback another foreign
# transaction.
($log, $xid) = run_transaction($node,
							   "SELECT test_inject_error('error', 'prepare', 'srv_2pc_1');",
							   "INSERT INTO ft_2pc_1 VALUES(1);
							   INSERT INTO ft_2pc_2 VALUES(1);");
like($log, qr/rollback $xid on srv_2pc_1/, "rollback on failed server");
like($log, qr/rollback prepared tx_$xid on srv_2pc_1/, "rollback prepared on failed server");
like($log, qr/rollback .* on srv_2pc_2/, "rollback on another server");
