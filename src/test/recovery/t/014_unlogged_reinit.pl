# Tests that unlogged tables are properly reinitialized after a crash.
#
# The behavior should be the same when restoring from a backup but that is not
# tested here.
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 12;

# Initialize node without replication settings
my $node = get_new_node('main');

$node->init;
$node->start;
my $pgdata = $node->data_dir;

# Create an unlogged table to test that forks other than init are not copied
$node->safe_psql('postgres', 'CREATE UNLOGGED TABLE base_unlogged (id int)');

my $baseUnloggedPath = $node->safe_psql('postgres',
	q{select pg_relation_filepath('base_unlogged')});

# Make sure main and init forks exist
ok(-f "$pgdata/${baseUnloggedPath}_init", 'init fork in base');
ok(-f "$pgdata/$baseUnloggedPath", 'main fork in base');

# Create unlogged tables in a tablespace
my $tablespaceDir = undef;
my $ts1UnloggedPath = undef;

$tablespaceDir = TestLib::tempdir . "/ts1";

mkdir($tablespaceDir)
	or BAIL_OUT("unable to mkdir '$tablespaceDir'");

$node->safe_psql('postgres',
	"CREATE TABLESPACE ts1 LOCATION '$tablespaceDir'");
$node->safe_psql('postgres',
	'CREATE UNLOGGED TABLE ts1_unlogged (id int) TABLESPACE ts1');

$ts1UnloggedPath = $node->safe_psql('postgres',
	q{select pg_relation_filepath('ts1_unlogged')});

# Make sure main and init forks exist
ok(-f "$pgdata/${ts1UnloggedPath}_init", 'init fork in tablespace');
ok(-f "$pgdata/$ts1UnloggedPath", 'main fork in tablespace');

# Crash the postmaster
$node->stop('immediate');

# Write forks to test that they are removed during recovery
append_to_file("$pgdata/${baseUnloggedPath}_vm", 'TEST_VM');
append_to_file("$pgdata/${baseUnloggedPath}_fsm", 'TEST_FSM');

# Remove main fork to test that it is recopied from init
unlink("$pgdata/${baseUnloggedPath}")
	or BAIL_OUT("unable to remove '${baseUnloggedPath}'");

# Write forks to test that they are removed by recovery
append_to_file("$pgdata/${ts1UnloggedPath}_vm", 'TEST_VM');
append_to_file("$pgdata/${ts1UnloggedPath}_fsm", 'TEST_FSM');

# Remove main fork to test that it is recopied from init
unlink("$pgdata/${ts1UnloggedPath}")
	or BAIL_OUT("unable to remove '${ts1UnloggedPath}'");

# Start the postmaster
$node->start;

# Check unlogged table in base
ok(-f "$pgdata/${baseUnloggedPath}_init", 'init fork still exists in base');
ok(-f "$pgdata/$baseUnloggedPath", 'main fork in base recreated at startup');
ok(!-f "$pgdata/${baseUnloggedPath}_vm", 'vm fork in base removed at startup');
ok(!-f "$pgdata/${baseUnloggedPath}_fsm",
	'fsm fork in base removed at startup');

# Drop unlogged table
$node->safe_psql('postgres', 'DROP TABLE base_unlogged');

# Check unlogged table in tablespace
ok(-f "$pgdata/${ts1UnloggedPath}_init",
	'init fork still exists in tablespace');
ok(-f "$pgdata/$ts1UnloggedPath",
	'main fork in tablspace recreated at startup');
ok(!-f "$pgdata/${ts1UnloggedPath}_vm",
	'vm fork in tablespace removed at startup');
ok(!-f "$pgdata/${ts1UnloggedPath}_fsm",
	'fsm fork in tablespace removed at startup');

# Drop unlogged table
$node->safe_psql('postgres', 'DROP TABLE ts1_unlogged');

# Drop tablespace
$node->safe_psql('postgres', 'DROP TABLESPACE ts1');
rmdir($tablespaceDir)
	or BAIL_OUT("unable to rmdir '$tablespaceDir'");
