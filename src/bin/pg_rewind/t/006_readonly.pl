# Test how pg_rewind reacts to read-only files in the data dirs.
# All such files should be ignored in the process.

use strict;
use warnings;
use TestLib;
use Test::More tests => 2;

use File::Copy;
use File::Find;

use RewindTest;

my $test_mode = "remote";

RewindTest::setup_cluster($test_mode);
RewindTest::start_master();
RewindTest::create_standby($test_mode);

# Create the same read-only file in standby and master
my $test_master_datadir = $node_master->data_dir;
my $test_standby_datadir = $node_standby->data_dir;
my $readonly_master = "$test_master_datadir/readonly_file";
my $readonly_standby = "$test_standby_datadir/readonly_file";

append_to_file($readonly_master, "in master");
append_to_file($readonly_standby, "in standby");
chmod 0400, $readonly_master, $readonly_standby;

RewindTest::promote_standby();

# Stop the master and run pg_rewind.
$node_master->stop;

my $master_pgdata   = $node_master->data_dir;
my $standby_pgdata  = $node_standby->data_dir;
my $standby_connstr = $node_standby->connstr('postgres');
my $tmp_folder      = TestLib::tempdir;

# Keep a temporary postgresql.conf for master node or it would be
# overwritten during the rewind.
copy(
	"$master_pgdata/postgresql.conf",
	"$tmp_folder/master-postgresql.conf.tmp");

# Including read-only data in the source and the target will
# cause pg_rewind to fail.
my $rewind_command = [   'pg_rewind',       "--debug",
		"--source-server", $standby_connstr,
		"--target-pgdata=$master_pgdata" ];

command_fails($rewind_command, 'pg_rewind fails with read-only');

# Now remove the read-only data on both sides, the data folder
# from the previous attempt should still be able to work.
unlink($readonly_master);
unlink($readonly_standby);
command_ok($rewind_command, 'pg_rewind passes without read-only');

# Now move back postgresql.conf with old settings
move("$tmp_folder/master-postgresql.conf.tmp",
	 "$master_pgdata/postgresql.conf");

# Plug-in rewound node to the now-promoted standby node
my $port_standby = $node_standby->port;
$node_master->append_conf('recovery.conf', qq(
primary_conninfo='port=$port_standby'
standby_mode=on
recovery_target_timeline='latest'
));

# Restart the master to check that rewind went correctly.
$node_master->start;

RewindTest::clean_rewind_test();

exit(0);
