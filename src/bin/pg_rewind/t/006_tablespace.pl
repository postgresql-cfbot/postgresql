use strict;
use warnings;
use File::Path qw(rmtree);
use TestLib;
use Test::More tests => 4;

use FindBin;
use lib $FindBin::RealBin;

use RewindTest;

my $tempdir = TestLib::tempdir;

sub run_test
{
	my $test_mode = shift;

	RewindTest::setup_cluster($test_mode);
	RewindTest::start_master();

	rmtree("$tempdir/inmaster");
	rmtree("$tempdir/instandby");
	rmtree("$tempdir/master_beforepromotion");
	rmtree("$tempdir/master_afterpromotion");
	rmtree("$tempdir/standby_afterpromotion");

	mkdir "$tempdir/inmaster";
	mkdir "$tempdir/instandby";

	# Create a tablespace in master.
	master_psql("CREATE TABLESPACE inmaster LOCATION '$tempdir/inmaster'");

	RewindTest::create_standby($test_mode, has_tablespace_mapping =>"$tempdir/inmaster=$tempdir/instandby");

	mkdir "$tempdir/master_beforepromotion";

	# Create a tablespace, it has to be droped before doing pg_rewind, or else pg_rewind will fail
	master_psql("CREATE TABLESPACE master_beforepromotion LOCATION '$tempdir/master_beforepromotion'");

	RewindTest::promote_standby();

	mkdir "$tempdir/master_afterpromotion";
	mkdir "$tempdir/standby_afterpromotion";

	# Create tablespaces in the old master and the new promoted standby.
	master_psql("CREATE TABLESPACE master_afterpromotion LOCATION '$tempdir/master_afterpromotion'");
	standby_psql("CREATE TABLESPACE standby_afterpromotion LOCATION '$tempdir/standby_afterpromotion'");
	# Drop tablespace in the new promoted standby, because pg_rewind can not handle this case.
	standby_psql("DROP TABLESPACE standby_afterpromotion");

	# The clusters are now diverged.

	RewindTest::run_pg_rewind($test_mode);

	# Check that the correct databases are present after pg_rewind.
	check_query(
		'SELECT spcname FROM pg_tablespace ORDER BY spcname',
		qq(inmaster
master_beforepromotion
pg_default
pg_global
),
		'tablespace names');

	RewindTest::clean_rewind_test();
	return;
}

# Run the test in both modes.
run_test('local');
run_test('remote');

exit(0);
