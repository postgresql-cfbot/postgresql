
# Copyright (c) 2021, PostgreSQL Global Development Group

use strict;
use warnings;
use TestLib;
use PostgresNode;
use Test::More tests => 23;

program_help_ok('pg_receivewal');
program_version_ok('pg_receivewal');
program_options_handling_ok('pg_receivewal');

# Set umask so test directories and files are created with default permissions
umask(0077);

my $primary = get_new_node('primary');
$primary->init(allows_streaming => 1);
$primary->start;

my $stream_dir = $primary->basedir . '/archive_wal';
mkdir($stream_dir);

# Sanity checks for command line options.
$primary->command_fails(['pg_receivewal'],
	'pg_receivewal needs target directory specified');
$primary->command_fails(
	[ 'pg_receivewal', '-D', $stream_dir, '--create-slot', '--drop-slot' ],
	'failure if both --create-slot and --drop-slot specified');
$primary->command_fails(
	[ 'pg_receivewal', '-D', $stream_dir, '--create-slot' ],
	'failure if --create-slot specified without --slot');
$primary->command_fails(
	[ 'pg_receivewal', '-D', $stream_dir, '--synchronous', '--no-sync' ],
	'failure if --synchronous specified with --no-sync');
$primary->command_fails(
	[
	  'pg_receivewal', '-D', $stream_dir, '--compression-method', 'lz4',
	  '--compress', '1'
	],
	'failure if --compression-method=lz4 specified with --compress');


# Slot creation and drop
my $slot_name = 'test';
$primary->command_ok(
	[ 'pg_receivewal', '--slot', $slot_name, '--create-slot' ],
	'creating a replication slot');
my $slot = $primary->slot($slot_name);
is($slot->{'slot_type'}, 'physical', 'physical replication slot was created');
is($slot->{'restart_lsn'}, '', 'restart LSN of new slot is null');
$primary->command_ok([ 'pg_receivewal', '--slot', $slot_name, '--drop-slot' ],
	'dropping a replication slot');
is($primary->slot($slot_name)->{'slot_type'},
	'', 'replication slot was removed');

# Generate some WAL.  Use --synchronous at the same time to add more
# code coverage.  Switch to the next segment first so that subsequent
# restarts of pg_receivewal will see this segment as full..
$primary->psql('postgres', 'CREATE TABLE test_table(x integer);');
$primary->psql('postgres', 'SELECT pg_switch_wal();');
my $nextlsn =
  $primary->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn();');
chomp($nextlsn);
$primary->psql('postgres',
	'INSERT INTO test_table VALUES (generate_series(1,100));');

# Stream up to the given position.
$primary->command_ok(
	[
		'pg_receivewal', '-D',     $stream_dir,     '--verbose',
		'--endpos',      $nextlsn, '--synchronous', '--no-loop'
	],
	'streaming some WAL with --synchronous');

# Check lz4 compression if available
SKIP:
{
	my $lz4 = $ENV{LZ4};

	skip "postgres was not build with LZ4 support", 3
		if (!check_pg_config("#define HAVE_LIBLZ4 1"));

	# Generate some WAL.
	$primary->psql('postgres', 'SELECT pg_switch_wal();');
	$nextlsn =
	  $primary->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn();');
	chomp($nextlsn);
	$primary->psql('postgres',
		'INSERT INTO test_table VALUES (generate_series(100,200));');
	$primary->psql('postgres', 'SELECT pg_switch_wal();');

	# Stream up to the given position
	$primary->command_ok(
		[
			'pg_receivewal', '-D',     $stream_dir,     '--verbose',
			'--endpos',      $nextlsn, '--compression-method=lz4'
		],
		'streaming some WAL with --compression-method=lz4');

	# Verify that the stored file is compressed
	my @lz4_wals = glob "$stream_dir/*.lz4";
	is(scalar(@lz4_wals), 1, 'one lz4 compressed WAL was created');

	# Verify that the stored file is readable if program lz4 is available
	skip "program lz4 is not found in your system", 1
	  if (!defined $lz4 || $lz4 eq '');

	my $can_decode = system_log($lz4, '-t', $lz4_wals[0]);
	is($can_decode, 0, "program lz4 can decode compressed WAL");
}

# Permissions on WAL files should be default
SKIP:
{
	skip "unix-style permissions not supported on Windows", 1
	  if ($windows_os);

	ok(check_mode_recursive($stream_dir, 0700, 0600),
		"check stream dir permissions");
}
