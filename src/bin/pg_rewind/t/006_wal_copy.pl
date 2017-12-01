#
# Test pg_rewind only copy needed WALs from the source.
#
use strict;
use warnings;
use TestLib;
use Test::More tests => 12;

use RewindTest;

sub run_test
{
	my $test_mode = shift;

	RewindTest::setup_cluster($test_mode);
	RewindTest::start_master();

	# Setup parameters for WAL reclaim 
	master_psql("ALTER SYSTEM SET checkpoint_timeout = '1d'");
	master_psql("ALTER SYSTEM SET min_wal_size = '80MB'");
	master_psql("ALTER SYSTEM SET wal_keep_segments = 4");
	master_psql("ALTER SYSTEM SET log_checkpoints = on");
	master_psql("SELECT pg_reload_conf()");

	RewindTest::create_standby($test_mode);

	# Create a test table and insert rows in master.
	master_psql("CREATE TABLE tbl1 (d text)");
	master_psql("INSERT INTO tbl1 VALUES ('in master, wal 1')");

	master_psql("SELECT pg_switch_wal()");
	master_psql("INSERT INTO tbl1 VALUES ('in master, wal 2')");

	master_psql("SELECT pg_switch_wal()");
	master_psql("INSERT INTO tbl1 VALUES ('in master, wal 3')");

	master_psql("SELECT pg_switch_wal()");
	master_psql("INSERT INTO tbl1 VALUES ('in master, wal 4')");

	master_psql("SELECT pg_switch_wal()");
	master_psql("INSERT INTO tbl1 VALUES ('in master, wal 5, checkpoint')");
	master_psql("CHECKPOINT");

	master_psql("SELECT pg_switch_wal()");
	master_psql("INSERT INTO tbl1 VALUES ('in master, wal 6, before promotion')");

	# Promote standby
	my $master_divergence_wal = $node_master->safe_psql("postgres",
		"SELECT pg_walfile_name(pg_current_wal_insert_lsn())");
	RewindTest::promote_standby();

	# Insert rows in master after promotion
	master_psql("INSERT INTO tbl1 VALUES ('in master, wal 6, after promotion')");

	master_psql("SELECT pg_switch_wal()");
	master_psql("INSERT INTO tbl1 VALUES ('in master, wal 7, after promotion')");

	master_psql("CHECKPOINT");

	# Insert rows in standby after promotion
	standby_psql("INSERT INTO tbl1 VALUES ('in standby, wal 6, after promotion')");

	standby_psql("SELECT pg_switch_wal()");
	standby_psql("INSERT INTO tbl1 VALUES ('in standby, wal 7, after promotion')");

	standby_psql("SELECT pg_switch_wal()");
	standby_psql("INSERT INTO tbl1 VALUES ('in standby, wal 8, after promotion')");	

	standby_psql("CHECKPOINT");

	# Check WALs before running pg_rewind
	master_psql("SELECT * from pg_ls_waldir()");
	print("master_divergence_wal: $master_divergence_wal\n");
	my $master_wal_count_before_divergence = $node_master->safe_psql("postgres",
		"SELECT count(*) FROM pg_ls_waldir() WHERE name ~ '^[0-9A-F]{24}\$' AND name < '$master_divergence_wal'");
	ok( $master_wal_count_before_divergence > 0, 'master_wal_count_before_divergence > 0');

	standby_psql("SELECT * from pg_ls_waldir()");
	my $standby_current_wal = $node_standby->safe_psql("postgres",
		"SELECT pg_walfile_name(pg_current_wal_insert_lsn())");
	print("standby_current_wal: $standby_current_wal\n");
	my $standby_reclaimed_wal_count = $node_standby->safe_psql("postgres",
		"SELECT count(*) FROM pg_ls_waldir() WHERE name ~ '^[0-9A-F]{24}\$' AND name > '$standby_current_wal'");
	ok( $standby_reclaimed_wal_count > 0, 'standby_reclaimed_wal_count > 0');

	# The accuracy of imodification from pg_ls_waldir() is seconds, so sleep one second
	sleep(1);
	my $pg_rewind_time = $node_master->safe_psql("postgres", "SELECT now()");
	print("pg_rewind_time: $pg_rewind_time\n");

	# Run pg_rewind and check
	RewindTest::run_pg_rewind($test_mode);

	master_psql("SELECT * from pg_ls_waldir()");

	check_query(
		'SELECT * FROM tbl1',
		qq(in master, wal 1
in master, wal 2
in master, wal 3
in master, wal 4
in master, wal 5, checkpoint
in master, wal 6, before promotion
in standby, wal 6, after promotion
in standby, wal 7, after promotion
in standby, wal 8, after promotion
),
		'table content');

	check_query(
		"SELECT count(*) FROM pg_ls_waldir() WHERE name ~ '^[0-9A-F]{24}\$' AND name < '$master_divergence_wal' AND modification >= '$pg_rewind_time'",
		qq(0
),
		'do not copy WALs before divergence');

	check_query(
		"SELECT count(*) FROM pg_ls_waldir() WHERE name ~ '^[0-9A-F]{24}\$' AND name > '$standby_current_wal' AND modification >= '$pg_rewind_time'",
		qq(0
),
		'do not copy reclaimed WALs from the source server');

	RewindTest::clean_rewind_test();
}

# Run the test in both modes
run_test('local');
run_test('remote');

exit(0);
