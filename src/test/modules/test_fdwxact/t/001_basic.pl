use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 11;

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
	my ($node, $prepsql, $sql, $endsql) = @_;

	$endsql = 'COMMIT' unless defined $endsql;

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

# Test the case where transaction attempting prepare the local transaction fails after
# preparing foreign transactions. The first attempt should be succeeded, but the second
# attempt will fail after preparing foreign transaction, and should rollback the prepared
# foreign transaction.
($log, $xid) = run_transaction($node, "",
							   "INSERT INTO t VALUES(1);
							   INSERT INTO ft_2pc_1 VALUES(1);",
							   "PREPARE TRANSACTION 'tx1'");
($log, $xid) = run_transaction($node, "",
							   "INSERT INTO t VALUES(1);
							   INSERT INTO ft_2pc_1 VALUES(1);",
							   "PREPARE TRANSACTION 'tx1'");
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
like($log, qr/rollback $xid on srv_2pc_2/, "rollback on another server");

# Inject an panic into prepare phase on srv_2pc_2. The server crashes after preparing both
# foreign transaction. After the restart, those transactions are recovered as in-doubt
# transactions. We check if the resolver process rollbacks those transaction after recovery.
($log, $xid) = run_transaction($node,
							   "SELECT test_inject_error('panic', 'prepare', 'srv_2pc_2');",
							   "INSERT INTO ft_2pc_1 VALUES(1);
							   INSERT INTO ft_2pc_2 VALUES(1);");
$node->restart();
$node->poll_query_until('postgres',
						"SELECT count(*) = 0 FROM pg_foreign_xacts")
  or die "Timeout while waiting for resolver process to resolve in-doubt transactions";
$log = TestLib::slurp_file($node->logfile);
like($log, qr/rollback prepared tx_[0-9]+ on srv_2pc_1/, "resolver rolled back in-doubt transaction");
like($log, qr/rollback prepared tx_[0-9]+ on srv_2pc_2/, "resolver rolled back in-doubt transaction");
truncate $node->logfile, 0;

# Inject an panic into commit phase on srv_2pc_1. The server crashes due to the panic
# error raised by resolver process during commit prepared foreign transaction on srv_2pc_1.
# After the restart, those transactions are recovered as in-doubt transactions. We check if
# the resolver process commits those transaction after recovery.
($log, $xid) = run_transaction($node,
							   "SELECT test_inject_error('panic', 'commit', 'srv_2pc_1');",
							   "INSERT INTO ft_2pc_1 VALUES(1);
							   INSERT INTO ft_2pc_2 VALUES(1);");
$node->restart();
$node->poll_query_until('postgres',
						"SELECT count(*) = 0 FROM pg_foreign_xacts")
  or die "Timeout while waiting for resolver process to resolve in-doubt transactions";
$log = TestLib::slurp_file($node->logfile);
like($log, qr/commit prepared tx_[0-9]+ on srv_2pc_1/, "resolver rolled back in-doubt transaction");
like($log, qr/commit prepared tx_[0-9]+ on srv_2pc_2/, "resolver rolled back in-doubt transaction");
truncate $node->logfile, 0;
