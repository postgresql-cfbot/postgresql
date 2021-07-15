# Copyright (c) 2021, PostgreSQL Global Development Group

# This test checks that logical replication table sync workers and apply workers
# respect privileges on database objects.
#
# We create a publisher node, a subscriber node, and a user named
# 'replication_manager' in role 'pg_logical_replication' which is intentionally
# not a superuser.  Logical replication subscriptions are created by this user,
# and consequently the table sync and apply workers run as this user.  If the
# database security surrounding logical replication is working correctly,
# logical replication of data to the subscriber node which causes the workers
# to modify tables will fail unless the replication_manager role has sufficient
# privileges.
#
# Failures in the logical replication workers result in infinite retry loops on
# the subscriber node.  To prevent the tests from getting stuck, we need to
# stop, clean, recreate and restart the nodes after each test where we expect
# such failures, thereby breaking the failure loops.
#

use strict;
use warnings;
use Time::HiRes qw(usleep);
use PostgresNode;
use TestLib;
use Scalar::Util qw(blessed);
use Test::More tests => 11;

my ($cmd, $publisher, $subscriber);

my $testno = 0;
sub create_fresh_nodes
{
	$testno++;

	$publisher = get_new_node("publisher_$testno");
	$publisher->init(allows_streaming => 'logical');
	$publisher->append_conf('postgresql.conf',
							'max_worker_processes = 12');
	$publisher->append_conf('postgresql.conf',
							'max_logical_replication_workers = 6');
	$publisher->start;

	$subscriber = get_new_node("subscriber_$testno");
	$subscriber->init;
	$subscriber->append_conf('postgresql.conf',
							 'max_worker_processes = 12');
	$subscriber->append_conf('postgresql.conf',
							 'max_logical_replication_workers = 6');
	$subscriber->start;

	# Create identical schema structure on both nodes
	$cmd = q(
		CREATE USER db_superuser WITH SUPERUSER;
		CREATE USER replication_manager IN ROLE pg_logical_replication;

		GRANT CREATE ON DATABASE postgres TO db_superuser;
		GRANT CREATE ON DATABASE postgres TO replication_manager;

		SET SESSION AUTHORIZATION db_superuser;

		-- Create a table owned by db_superuser with privileges granted to
		-- replication_manager
		CREATE TABLE shared_tbl (i INTEGER);
		CREATE UNIQUE INDEX shared_tbl_idx
			ON shared_tbl(i);
		ALTER TABLE shared_tbl REPLICA IDENTITY FULL;
		REVOKE ALL PRIVILEGES ON TABLE shared_tbl FROM PUBLIC;
		GRANT INSERT, UPDATE, DELETE, TRUNCATE
			ON shared_tbl
			TO replication_manager;

		-- Create a table owned by db_superuser without privileges granted to
		-- anybody else
		CREATE TABLE private_tbl (i INTEGER);
		CREATE UNIQUE INDEX private_tbl_idx
			ON private_tbl(i);
		ALTER TABLE private_tbl REPLICA IDENTITY FULL;
		REVOKE ALL PRIVILEGES ON TABLE private_tbl FROM PUBLIC;

		RESET SESSION AUTHORIZATION;
	);
	$publisher->safe_psql('postgres', $cmd);
	$subscriber->safe_psql('postgres', $cmd);
}

sub configure_logical_replication
{
	my %options = @_;

	# On the subscriber side only, create a logging mechanism to track what
	# operations are performed on which tables and by which users.
	$subscriber->safe_psql('postgres', q(
		CREATE TABLE log (
			i INTEGER,
			table_name TEXT,
			op TEXT,
			current_username TEXT,
			session_username TEXT
		);
		GRANT ALL PRIVILEGES ON log TO public;
		CREATE FUNCTION log_row_trigger_func() RETURNS TRIGGER AS $$
		BEGIN
			INSERT INTO public.log
				VALUES (NEW.i, TG_TABLE_NAME, TG_OP, CURRENT_USER, SESSION_USER);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		GRANT ALL PRIVILEGES ON FUNCTION log_row_trigger_func() TO public;

		SET SESSION AUTHORIZATION db_superuser;
		CREATE TRIGGER shared_log_row_trigger
			AFTER INSERT OR UPDATE ON shared_tbl
			FOR EACH ROW EXECUTE FUNCTION log_row_trigger_func();
		ALTER TABLE shared_tbl
			ENABLE ALWAYS TRIGGER shared_log_row_trigger;
		CREATE TRIGGER private_log_row_trigger
			AFTER INSERT OR UPDATE ON private_tbl
			FOR EACH ROW EXECUTE FUNCTION log_row_trigger_func();
		ALTER TABLE private_tbl
			ENABLE ALWAYS TRIGGER private_log_row_trigger;
		RESET SESSION AUTHORIZATION;
		));

	my $publisher_connstr = $publisher->connstr . ' dbname=postgres';
	$publisher->safe_psql('postgres', qq(
		SET SESSION AUTHORIZATION replication_manager;
		CREATE PUBLICATION private_tbl_pub
			FOR TABLE private_tbl;
		CREATE PUBLICATION shared_tbl_pub
			FOR TABLE shared_tbl;
	));

	$subscriber->safe_psql('postgres', qq(
		SET SESSION AUTHORIZATION replication_manager;
		CREATE SUBSCRIPTION shared_tbl_sub
			CONNECTION '$publisher_connstr'
			PUBLICATION shared_tbl_pub;
	));

	if ($options{'attempt_private_replication'})
	{
		$subscriber->safe_psql('postgres', qq(
			SET SESSION AUTHORIZATION replication_manager;
			CREATE SUBSCRIPTION private_tbl_sub
				CONNECTION '$publisher_connstr'
				PUBLICATION private_tbl_pub;
		));
	}
}

sub destroy_nodes
{
	$subscriber->teardown_node();
	$publisher->teardown_node();
	$subscriber->clean_node();
	$publisher->clean_node();
}

sub truncate_log
{
	$subscriber->safe_psql('postgres', qq(TRUNCATE log));
}

sub truncate_test_data
{
	$subscriber->safe_psql('postgres', qq(
		TRUNCATE shared_tbl;
		TRUNCATE private_tbl;
		TRUNCATE log));
}

# Wait for a subscription to finish replicating or to fail in the attempt.
#
# failure_re: a regular expression that should match the subscriber node's log
#             if and only if the replication failed.
# success_sql: a sql query which will return true on the subscriber node if and
#              only if the data being awaited replicated successfully.
# expected: "fail" or "success", depending on whether we expect the replication
#           attempt to fail or to succeed.
#
sub expect_replication
{
	my ($failure_re, $success_sql, $expected, $testname) = @_;

	for (1..1800)	# 180 seconds max
	{
		# Has the subscription failed?
		my $contents = TestLib::slurp_file($subscriber->logfile);
		if ($contents =~ m/$failure_re/ms)
		{
			is ("fail", $expected, $testname);
			return;
		}

		# Has the subscription caught up?
		if ($subscriber->safe_psql('postgres', $success_sql) eq 't')
		{
			is ("success", $expected, $testname);
			return;
		}

		# Wait 0.1 second before retrying.
		usleep(100_000);
	}

	diag("replication_fails timed out waiting for failure matching /$failure_re/" .
		 "or for true value returned from '$success_sql'");
}

sub expect_log
{
	my ($expected, $testname) = @_;

	is ($subscriber->safe_psql('postgres', qq(SELECT * FROM log)),
		$expected, $testname);
}

sub expect_value
{
	my ($table, $expected, $testname) = @_;

	is ($subscriber->safe_psql('postgres', qq(SELECT i FROM $table)),
		$expected, $testname);
}

#
# Check that 'replication_manager' can replicate INSERT to the shared table
#
create_fresh_nodes();
configure_logical_replication();
$publisher->wait_for_catchup('shared_tbl_sub');
$publisher->safe_psql('postgres', qq(
	SET SESSION AUTHORIZATION db_superuser;
	INSERT INTO shared_tbl (i) VALUES (1);
));
expect_replication(
	qr/ERROR/,		# We should not see any ERROR message
	qq(SELECT COUNT(1) > 0 FROM shared_tbl),
	"success",
	"replication can propogate inserts into shared_tbl");
expect_log(
	"1|shared_tbl|INSERT|replication_manager|replication_manager",
	"successful replication of INSERT to shared_tbl is logged");
expect_value(
	"shared_tbl", "1",
	"shared_tbl contains expected row after replication of INSERT");

#
# Check that 'replication_manager' can replicate UPDATE to the shared table
#
truncate_log();
$publisher->safe_psql('postgres', qq(
	SET SESSION AUTHORIZATION db_superuser;
	UPDATE shared_tbl SET i = i + 1;
));
expect_replication(
	qr/ERROR/,		# We should not see any ERROR message
	qq(SELECT COUNT(1) > 0 FROM shared_tbl WHERE i = 2),
	"success",
	"replication can propogate updates into shared_tbl");
expect_log(
	"2|shared_tbl|UPDATE|replication_manager|replication_manager",
	"successful replication of UPDATE to shared_tbl is logged");
expect_value(
	"shared_tbl", "2",
	"shared_tbl contains expected row after replication of UPDATE");

#
# Check that having privileges to replicate into a table allows triggering
# functions which replication_manager would otherwise not have permission to
# execute.  This may seem wrong, but it is how function execution privileges
# are handled vis-a-vis triggers under non-logical-replication circumstances,
# and it would be more surprising for logical replication to diverge from this
# behavior than to adhere to it.
#
truncate_test_data();
$subscriber->safe_psql('postgres', q(
	SET SESSION AUTHORIZATION db_superuser;
	CREATE FUNCTION db_superuser_func() returns trigger as $$
	BEGIN
		NEW.i = NEW.i + 1;
		RETURN NEW;
	END;
	$$ LANGUAGE plpgsql;
	REVOKE ALL PRIVILEGES ON FUNCTION db_superuser_func() FROM PUBLIC;
	CREATE TRIGGER escalation_trig
		BEFORE INSERT OR UPDATE
		ON shared_tbl
		FOR EACH ROW EXECUTE FUNCTION db_superuser_func();
	ALTER TABLE shared_tbl
		ENABLE ALWAYS TRIGGER escalation_trig;
	GRANT INSERT ON shared_tbl TO replication_manager;
));
$publisher->safe_psql('postgres', qq(
	SET SESSION AUTHORIZATION db_superuser;
	INSERT INTO shared_tbl (i) VALUES (3);
));
expect_replication(
	qr/ERROR/,		# We should not see any ERROR message
	qq(SELECT COUNT(1) > 0 FROM shared_tbl WHERE i IN (3, 4)),
	"success",
	"replication performs inserts despite lacking trigger function privileges");
expect_log(
	"4|shared_tbl|INSERT|replication_manager|replication_manager",
	"successful replication and triggered modification of INSERT into shared_tbl is logged");
expect_value(
	"shared_tbl", "4",
	"shared_tbl contains expected, trigger-altered value");
truncate_test_data();

#
# Check that table sync workers cannot INSERT into the private table
#
destroy_nodes();
create_fresh_nodes();
$publisher->safe_psql('postgres', qq(
SET SESSION AUTHORIZATION db_superuser;
INSERT INTO private_tbl (i) VALUES (6);
));
configure_logical_replication(
	attempt_private_replication => 1);
expect_replication(
	qr/ERROR/,
	qq(SELECT COUNT(1) > 0 FROM private_tbl WHERE i = 6),
	"fail",
	"table sync workers cannot insert into private table");
expect_value(
	"private_tbl", "",
	"private_tbl empty after failed table sync");
