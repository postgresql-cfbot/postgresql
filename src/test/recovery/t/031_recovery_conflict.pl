# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Test that connections to a hot standby are correctly canceled when a recovery conflict is detected
# Also, test that statistics in pg_stat_database_conflicts are populated correctly

# TODO: add a test for deadlock recovery conflicts.

# recovery conflicts
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $psql_timeout = IPC::Run::timer($PostgreSQL::Test::Utils::timeout_default);
# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(
	allows_streaming => 1,
	auth_extra       => [ '--create-role', 'repl_role' ]);

my $tablespace1 = "test_recovery_conflict_tblspc";

$node_primary->append_conf(
	'postgresql.conf', qq[
allow_in_place_tablespaces = on
log_temp_files = 0

# wait some to test the wait paths as well, but not long for obvious reasons
max_standby_streaming_delay = 50ms

temp_tablespaces = $tablespace1
# Some of the recovery conflict logging code only gets exercised after
# deadlock_timeout. The test doesn't rely on that additional output, but it's
# nice to get some minimal coverage of that code.
log_recovery_conflict_waits = on
deadlock_timeout = 10ms
]);
$node_primary->start;

my $backup_name = 'my_backup';

$node_primary->safe_psql('postgres',
	qq[CREATE TABLESPACE $tablespace1 LOCATION '']);

# Take backup
$node_primary->backup($backup_name);

# Create streaming standby linking to primary
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->start;

my $test_db = "test_db";

# Make a new database for these tests so that it can also be used for the
# database recovery conflict test.
$node_primary->safe_psql(
	'postgres',
	qq[
		CREATE DATABASE $test_db;
					   ]);

# Make table for first few recovery conflict types under test
my $table1 = "test_recovery_conflict_table1";

$node_primary->safe_psql($test_db, qq[CREATE TABLE ${table1}(a int, b int);]);

$node_primary->safe_psql($test_db,
	qq[INSERT INTO $table1 SELECT i % 3, 0 FROM generate_series(1,20) i]);

# Wait for catchup so database and tables exists when establishing connection
my $primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

# Make a psql connection on the standby
my %psql_standby = ('stdin' => '', 'stdout' => '', 'stderr' => '');
$psql_standby{run} = IPC::Run::start(
	[ 'psql', '-XA', '-f', '-', '-d', $node_standby->connstr($test_db) ],
	'<',
	\$psql_standby{stdin},
	'>',
	\$psql_standby{stdout},
	'2>',
	\$psql_standby{stderr},
	$psql_timeout);


my $nconflicts = 0;

# RECOVERY CONFLICT 1: Buffer pin conflict
$nconflicts++;

# Aborted INSERT on primary
$node_primary->safe_psql(
	$test_db,
	qq[
	BEGIN;
	INSERT INTO $table1 VALUES (1,0);
	ROLLBACK;
	]);

$primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

my $cursor1 = "test_recovery_conflict_cursor";

# DECLARE and use a cursor on standby, causing buffer with the only block of
# the relation to be pinned on the standby
$psql_standby{stdin} .= qq[
		BEGIN;
		DECLARE $cursor1 CURSOR FOR SELECT b FROM $table1;
		FETCH FORWARD FROM $cursor1;
		];
# FETCH FORWARD should have returned a 0 since all values of b in the table are
# 0
ok( pump_until(
		$psql_standby{run},     $psql_timeout,
		\$psql_standby{stdout}, qr/^0$/m,),
	"cursor");

# Reset the streams to avoid false positive test results in later tests
($psql_standby{stdin}, $psql_standby{stderr}, $psql_standby{stdout}) =
  ('', '', '');

# Get log location before taking action to cause disconnect of connections on
# standby
my $log_location = -s $node_standby->logfile;

# VACUUM on the primary
$node_primary->safe_psql($test_db, qq[VACUUM $table1;]);

# Wait for catchup. Existing connection will be terminated before replay is
# finished, so waiting for catchup ensures that there is no race between
# encountering the recovery conflict which causes the disconnect and checking
# the logfile for the terminated connection.
$primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

cmp_ok(
	$node_standby->wait_for_log(
		qr/User was holding shared buffer pin for too long/,
		$log_location),
	'>',
	$log_location,
	'Logfile should contain details of a terminated connection due to the buffer pin recovery conflict.'
);
reconnect_and_clear(\%psql_standby, $psql_timeout);

is( $node_standby->safe_psql(
		$test_db,
		qq[SELECT confl_bufferpin FROM pg_stat_database_conflicts WHERE datname='$test_db';]
	),
	1,
	'Check that buffer pin conflict is captured in stats on standby.');

# RECOVERY CONFLICT 2: Snapshot conflict
$nconflicts++;

# INSERT new data
$node_primary->safe_psql($test_db,
	qq[INSERT INTO $table1 SELECT i, 0 FROM generate_series(1,20) i]);

# Wait for catchup
$primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

# DECLARE and FETCH from cursor on the standby
$psql_standby{stdin} .= qq[
		BEGIN;
		DECLARE $cursor1 CURSOR FOR SELECT b FROM $table1;
		FETCH FORWARD FROM $cursor1;
		];
ok( pump_until(
		$psql_standby{run},     $psql_timeout,
		\$psql_standby{stdout}, qr/^0$/m,),
	"cursor");

# Do some HOT updates
$node_primary->safe_psql(
	$test_db,
	qq[
		UPDATE $table1 SET a = a + 1 WHERE a > 2;
					   ]);

# Update log location
$log_location = -s $node_standby->logfile;

# VACUUM, pruning those dead tuples
$node_primary->safe_psql(
	$test_db,
	qq[
		VACUUM $table1;
					   ]);

# Wait for attempted replay of PRUNE records
$primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

cmp_ok(
	$node_standby->wait_for_log(
		qr/User query might have needed to see row versions that must be removed/,
		$log_location),
	'>',
	$log_location,
	'Logfile should contain details of a terminated connection due to snapshot recovery conflict.'
);
reconnect_and_clear(\%psql_standby, $psql_timeout);

is( $node_standby->safe_psql(
		$test_db,
		qq[SELECT confl_snapshot FROM pg_stat_database_conflicts WHERE datname='$test_db';]
	),
	1,
	'Check that snapshot conflict was captured in stats on standby.');


# RECOVERY CONFLICT 3: Lock conflict
$nconflicts++;

# DECLARE and FETCH from cursor on the standby
$psql_standby{stdin} .= qq[
		BEGIN;
		DECLARE $cursor1 CURSOR FOR SELECT b FROM $table1;
		FETCH FORWARD FROM $cursor1;
		];
ok( pump_until(
		$psql_standby{run},     $psql_timeout,
		\$psql_standby{stdout}, qr/^0$/m,),
	"cursor");

# Update log location
$log_location = -s $node_standby->logfile;

# DROP TABLE containing block which standby has in a pinned buffer
$node_primary->safe_psql(
	$test_db,
	qq[
		DROP TABLE $table1;
					   ]);

# Wait for catchup
$primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

cmp_ok(
	$node_standby->wait_for_log(
		qr/User was holding a relation lock for too long/,
		$log_location),
	'>',
	$log_location,
	'Logfile should contain details of a terminated connection due to lock recovery conflict.'
);
reconnect_and_clear(\%psql_standby, $psql_timeout);

is( $node_standby->safe_psql(
		$test_db,
		qq[SELECT confl_lock FROM pg_stat_database_conflicts WHERE datname='$test_db';]
	),
	1,
	'Check that lock conflict is captured in stats on standby.');


# RECOVERY CONFLICT 4: Tablespace conflict
$nconflicts++;

# DECLARE a cursor for a query which, with sufficiently low work_mem, will
# spill tuples into temp files in the temporary tablespace created during
# setup.
$psql_standby{stdin} .= qq[
		BEGIN;
		SET work_mem = '64kB';
		DECLARE $cursor1 CURSOR FOR
		SELECT count(*) FROM generate_series(1,6000);
		FETCH FORWARD FROM $cursor1;
		];
ok( pump_until(
		$psql_standby{run},     $psql_timeout,
		\$psql_standby{stdout}, qr/^\(1 row\)$/m,),
	"cursor");

# Update log location
$log_location = -s $node_standby->logfile;

# Drop the tablespace currently containing spill files for the query on the
# standby
$node_primary->safe_psql(
	$test_db,
	qq[
		DROP TABLESPACE $tablespace1;
					   ]);

# Wait for catchup
$primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

cmp_ok(
	$node_standby->wait_for_log(
		qr/User was or might have been using tablespace that must be dropped/,
		$log_location),
	'>',
	$log_location,
	'Logfile should contain details of a terminated connection due to tablespace recovery conflict.'
);
reconnect_and_clear(\%psql_standby, $psql_timeout);

is( $node_standby->safe_psql(
		$test_db,
		qq[SELECT confl_tablespace FROM pg_stat_database_conflicts WHERE datname='$test_db';]
	),
	1,
	'Check that tablespace conflict was captured in stats on standby.');

is( $node_standby->safe_psql(
		$test_db,
		qq[SELECT conflicts FROM pg_stat_database WHERE datname='$test_db';]),
	$nconflicts,
	qq[Check that all $nconflicts recovery conflicts have been captured in pg_stat_database.]
);


# RECOVERY CONFLICT 5: Database conflict
$nconflicts++;

# Update log location
$log_location = -s $node_standby->logfile;

$node_primary->safe_psql(
	'postgres',
	qq[
		DROP DATABASE $test_db;
					   ]);

# Wait for catchup
$primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

cmp_ok(
	$node_standby->wait_for_log(
		qr/User was connected to a database that must be dropped/,
		$log_location),
	'>',
	$log_location,
	'Logfile should contain details of a terminated connection due to database recovery conflict.'
);

# explicitly shut down psql instances gracefully - to avoid hangs
# or worse on windows
$psql_standby{stdin} .= "\\q\n";
$psql_standby{run}->finish;

$node_standby->stop();
$node_primary->stop();

sub reconnect_and_clear
{
	# Note that all of the entries in the hash passed in must match those in
	# this subroutine exactly
	my ($psql, $psql_timeout) = @_;

	$$psql{stdin} .= "\\q\n";
	$$psql{run}->finish;

	# Run
	$$psql{run}->run();

	# Reset streams
	($$psql{stdin}, $$psql{stderr}, $$psql{stdout}) = ('', '', '');

	# Run query to ensure connection has been re-established
	$$psql{stdin} .= qq[
			SELECT 1;
			];
	die unless pump_until($$psql{run}, $psql_timeout, \$$psql{stdout}, qr/^1$/m,);
}

done_testing();
