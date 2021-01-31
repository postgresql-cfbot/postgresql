# Prohibit archive recovery when the server detects WAL generated with wal_level=minimal
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 7;
use Config;
use Time::HiRes qw(usleep);

# Initialize a node
my $node = get_new_node('orig');
my $backup_name = 'my_backup';
my $replica_config = q[
wal_level = replica
archive_mode = on
max_wal_senders = 10
hot_standby = off
];

# Start up the server with wal_level = replica
$node->init(has_archiving => 1);
$node->append_conf('postgresql.conf', $replica_config);
$node->start;

# Check the wal_level and get a backup
check_wal_level('replica', 'wal_level is replica at first');
$node->backup($backup_name);

# Change the wal_level from replica to minimal
$node->append_conf(
	'postgresql.conf', q[
wal_level = minimal
archive_mode = off
max_wal_senders = 0
]);
$node->restart;
check_wal_level('minimal', 'wal_level has become minimal');

# Make the wal_level back to replica
$node->append_conf('postgresql.conf', $replica_config);
$node->restart;
check_wal_level('replica', 'wal_level went back to replica again');
$node->stop;

# Execute an archive recovery in standby mode
my $new_node = get_new_node('new_node');
$new_node->init_from_backup(
	$node, $backup_name,
	has_restoring => 1);

# Check if standby.signal exists
my $pgdata = $new_node->data_dir;
ok (-f "${pgdata}/standby.signal", 'standby.signal was created');

run_log(
	[
	 'pg_ctl',               '-D', $new_node->data_dir, '-l',
	 $new_node->logfile, 'start'
	]);

# Wait up to 180s for postgres to terminate
foreach my $i (0 .. 1800)
{
    last if !-f $new_node->data_dir . '/postmaster.pid';
    usleep(100_000);
}

# Confirm that the archive recovery fails with an error
my $logfile = slurp_file($new_node->logfile());
ok( $logfile =~
      qr/FATAL:  WAL was generated with wal_level=minimal, cannot continue recovering/,
    'Archive recovery fails in standby mode with WAL generated during wal_level=minimal');

# This protection shold apply to recovery mode
my $another_node = get_new_node('another_node');
$another_node->init_from_backup(
	$node, $backup_name,
	has_restoring => 1, standby => 0);

# Check if recovery.signal exists
my $path = $another_node->data_dir;
ok (-f "${path}/recovery.signal", 'recovery.signal was created');
run_log(
	[
	 'pg_ctl',               '-D', $another_node->data_dir, '-l',
	 $another_node->logfile, 'start'
	]);

foreach my $i (0 .. 1800)
{
    last if !-f $another_node->data_dir . '/postmaster.pid';
    usleep(100_000);
}

my $log = slurp_file($another_node->logfile());
ok( $log =~
	qr/FATAL:  WAL was generated with wal_level=minimal, cannot continue recovering/,
    'Archive recovery fails in recovery mode with WAL generated during wal_level=minimal');

sub check_wal_level
{
	my ($target_wal_level, $explanation) = @_;

	is( $node->safe_psql(
			'postgres', q{show wal_level}),
        $target_wal_level,
        $explanation);
}
