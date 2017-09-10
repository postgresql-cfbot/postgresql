use strict;
use warnings;
use TestLib;
use PostgresNode;
use Test::More tests => 15;

program_help_ok('pg_receivewal');
program_version_ok('pg_receivewal');
program_options_handling_ok('pg_receivewal');

my $primary = get_new_node('primary');
$primary->init(allows_streaming => 1);
$primary->start;

my $stream_dir = $primary->basedir . '/archive_wal';
mkdir($stream_dir);

# Sanity checks for command line options.
$primary->command_fails(['pg_receivewal'],
	'pg_receivewal needs target directory specified');
$primary->command_fails(['pg_receivewal', '-D', $stream_dir, '--create-slot',
			   '--drop-slot'],
	'failure if both --create-slot and --drop-slot specified');
$primary->command_fails(['pg_receivewal', '-D', $stream_dir, '--create-slot'],
	'failure if --create-slot defined without --slot');

# Slot creation and drop
my $slot_name = 'test';
$primary->command_ok(['pg_receivewal', '--slot', $slot_name, '--create-slot' ],
	'creation of replication slot');
$primary->command_ok(['pg_receivewal', '--slot', $slot_name, '--drop-slot' ],
	'drop of replication slot');

# Generate some WAL using non-compression mode. Use --synchronous
# at the same time to add more code coverage. Switch to the next
# segment first so as subsequent restarts of pg_receivewal will
# see this segment as full and non-compressed.
$primary->psql('postgres', 'CREATE TABLE test_table(x integer);');
$primary->psql('postgres', 'SELECT pg_switch_wal();');
my $nextlsn =
	$primary->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn();');
chomp($nextlsn);
$primary->psql('postgres',
	'INSERT INTO test_table VALUES (generate_series(1,100));');

# Stream up to the given position.
$primary->command_ok(
	[   'pg_receivewal', '-D', $stream_dir, '--verbose', '--endpos',
		$nextlsn, '--synchronous', '--no-loop' ],
	'streaming some WAL with --synchronous and without compression');

# Check if build supports compression with libz, in which case the following
# command would pass or fail depending on its support.
my $lz_support = $primary->safe_psql('postgres',
	"SELECT setting ~ '-lz ' from pg_config WHERE name = 'LIBS'");
chomp($lz_support);
if ($lz_support eq 't')
{
	# Now generate more WAL, switch to a new segment and stream
	# changes using the compression mode.
	$primary->psql('postgres',
		'INSERT INTO test_table VALUES (generate_series(1,100));');
	$primary->psql('postgres', 'SELECT pg_switch_wal();');
	$nextlsn =
		$primary->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn();');
	chomp($nextlsn);
	$primary->psql('postgres',
		'INSERT INTO test_table VALUES (generate_series(1,100));');

	$primary->command_ok(
		[   'pg_receivewal', '-D', $stream_dir, '--verbose', '--endpos',
			$nextlsn, '--compress', '1', '--no-loop' ],
		'streaming some WAL with compression');
}
else
{
	$primary->command_fails(
		[   'pg_receivewal', '-D', $stream_dir, '--verbose', '--endpos',
			$nextlsn, '--compress', '1', '--no-loop' ],
		'compression not supported');
}
