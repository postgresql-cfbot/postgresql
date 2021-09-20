
# Copyright (c) 2021, PostgreSQL Global Development Group

use strict;
use warnings;
use TestLib;
use PostgresNode;
use Test::More tests => 34;

program_help_ok('pg_receivewal');
program_version_ok('pg_receivewal');
program_options_handling_ok('pg_receivewal');

# Set umask so test directories and files are created with default permissions
umask(0077);

my $primary = PostgresNode->new('primary');
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
$primary->command_fails_like(
	[
		'pg_receivewal', '-D', $stream_dir, '--compression-method', 'none',
		'--compress',    '1'
	],
	qr/\Qpg_receivewal: error: can only use --compress with --compression-method=gzip/,
	'failure if --compression-method=none specified with --compress');

# Slot creation and drop
my $slot_name = 'test';
$primary->command_ok(
	[ 'pg_receivewal', '--slot', $slot_name, '--create-slot' ],
	'creating a replication slot');
my $slot = $primary->slot($slot_name);
is($slot->{'slot_type'}, 'physical', 'physical replication slot was created');
is($slot->{'restart_lsn'}, '',       'restart LSN of new slot is null');
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

# Stream up to the given position.  This is necessary to have a fixed
# started point for the next commands done in this test, with or without
# compression involved.
$primary->command_ok(
	[
		'pg_receivewal', '-D',     $stream_dir,     '--verbose',
		'--endpos',      $nextlsn, '--synchronous', '--no-loop'
	],
	'streaming some WAL with --synchronous');

# Verify that one partial file was generated and keep track of it
my @partial_wals = glob "$stream_dir/*\.partial";
is(scalar(@partial_wals), 1, "one partial WAL segment was created");

# Check ZLIB compression if available.
SKIP:
{
	skip "postgres was not built with ZLIB support", 5
	  if (!check_pg_config("#define HAVE_LIBZ 1"));

	# Generate more WAL worth one completed, compressed, segment.
	$primary->psql('postgres', 'SELECT pg_switch_wal();');
	$nextlsn =
	  $primary->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn();');
	chomp($nextlsn);
	$primary->psql('postgres',
		'INSERT INTO test_table VALUES (generate_series(100,200));');

	# Note the trailing whitespace after the value of --compress, that is
	# a valid value.
	$primary->command_ok(
		[
			'pg_receivewal',        '-D',
			$stream_dir,            '--verbose',
			'--endpos',             $nextlsn,
			'--compression-method', 'gzip',
			'--compress',           '1 ',
			'--no-loop'
		],
		"streaming some WAL using ZLIB compression");

	# Verify that the stored files are generated with their expected
	# names.
	my @zlib_wals = glob "$stream_dir/*.gz";
	is(scalar(@zlib_wals), 1,
		"one WAL segment compressed with ZLIB was created");
	my @zlib_partial_wals = glob "$stream_dir/*.gz.partial";
	is(scalar(@zlib_partial_wals),
		1, "one partial WAL segment compressed with ZLIB was created");

	# Verify that the start streaming position is computed correctly by
	# comparing it with the partial file generated previously.  The name
	# of the previous partial, now-completed WAL segment is updated, keeping
	# its base number.
	$partial_wals[0] =~ s/\.partial$/.gz/;
	is($zlib_wals[0] eq $partial_wals[0],
		1, "one partial WAL segment is now completed");
	# Update the list of partial wals with the current one.
	@partial_wals = @zlib_partial_wals;

	# Check the integrity of the completed segment, if gzip is a command
	# available.
	my $gzip = $ENV{GZIP_PROGRAM};
	skip "program gzip is not found in your system", 1
	  if ( !defined $gzip
		|| $gzip eq ''
		|| system_log($gzip, '--version') != 0);

	my $gzip_is_valid = system_log($gzip, '--test', @zlib_wals);
	is($gzip_is_valid, 0,
		"gzip verified the integrity of compressed WAL segments");
}

# Check LZ4 compression if available
SKIP:
{
	skip "postgres was not built with LZ4 support", 5
	  if (!check_pg_config("#define HAVE_LIBLZ4 1"));

	# Generate more WAL including one completed, compressed segment.
	$primary->psql('postgres', 'SELECT pg_switch_wal();');
	$nextlsn =
	  $primary->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn();');
	chomp($nextlsn);
	$primary->psql('postgres',
		'INSERT INTO test_table VALUES (generate_series(201,300));');

	# Stream up to the given position
	$primary->command_ok(
		[
			'pg_receivewal', '-D',
			$stream_dir,     '--verbose',
			'--endpos',      $nextlsn,
			'--no-loop',     '--compression-method',
			'lz4'
		],
		'streaming some WAL using --compression-method=lz4');

	# Verify that the stored files are generated with their expected
	# names.
	my @lz4_wals = glob "$stream_dir/*.lz4";
	is(scalar(@lz4_wals), 1,
		"one WAL segment compressed with LZ4 was created");
	my @lz4_partial_wals = glob "$stream_dir/*.lz4.partial";
	is(scalar(@lz4_partial_wals),
		1, "one partial WAL segment compressed with LZ4 was created");

	# Verify that the start streaming position is computed correctly by
	# comparing it with the partial file generated previously.  The name
	# of the previous partial, now-completed WAL segment is updated, keeping
	# its base number.
	$partial_wals[0] =~ s/(\.gz)?\.partial$/.lz4/;
	is($lz4_wals[0] eq $partial_wals[0],
		1, "one partial WAL segment is now completed");
	# Update the list of partial wals with the current one.
	@partial_wals = @lz4_partial_wals;

	# Check the integrity of the completed segment, if LZ4 is an available
	# command.
	my $lz4 = $ENV{LZ4};
	skip "program lz4 is not found in your system", 1
	  if ( !defined $lz4
		|| $lz4 eq ''
		|| system_log($lz4, '--version') != 0);

	my $lz4_is_valid = system_log($lz4, '-t', @lz4_wals);
	is($lz4_is_valid, 0,
		"lz4 verified the integrity of compressed WAL segments");
}

# Verify that the start streaming position is computed and that the value is
# correct regardless of whether any compression is available.
$primary->psql('postgres', 'SELECT pg_switch_wal();');
$nextlsn =
  $primary->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn();');
chomp($nextlsn);
$primary->psql('postgres',
	'INSERT INTO test_table VALUES (generate_series(301,400));');
$primary->command_ok(
	[
		'pg_receivewal', '-D',     $stream_dir, '--verbose',
		'--endpos',      $nextlsn, '--no-loop'
	],
	"streaming some WAL");

$partial_wals[0] =~ s/(\.gz|\.lz4)?.partial//;
ok(-e $partial_wals[0], "check that previously partial WAL is now complete");

# Permissions on WAL files should be default
SKIP:
{
	skip "unix-style permissions not supported on Windows", 1
	  if ($windows_os);

	ok(check_mode_recursive($stream_dir, 0700, 0600),
		"check stream dir permissions");
}
