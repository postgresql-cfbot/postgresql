# Copyright (c) 2021-2024, PostgreSQL Global Development Group
#
# Verify that, if we have a backup manifest for the final backup directory,
# we can detect missing files.

use strict;
use warnings FATAL => 'all';
use File::Compare;
use File::Path qw(rmtree);
use File::Copy;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Set up a new database instance.
my $node1 = PostgreSQL::Test::Cluster->new('node1');
$node1->init(has_archiving => 1, allows_streaming => 1);
$node1->append_conf('postgresql.conf', 'summarize_wal = on');
$node1->start;

# Set up another new database instance.  force_initdb is used because
# we want it to be a separate cluster with a different system ID.
my $node2 = PostgreSQL::Test::Cluster->new('node2');
$node2->init(force_initdb => 1, has_archiving => 1, allows_streaming => 1);
$node2->append_conf('postgresql.conf', 'summarize_wal = on');
$node2->start;

# Take a full backup from node1.
my $backup1path = $node1->backup_dir . '/backup1';
$node1->command_ok(
	[ 'pg_basebackup', '-D', $backup1path, '--no-sync', '-cfast' ],
	"full backup from node1");

# Now take an incremental backup.
my $backup2path = $node1->backup_dir . '/backup2';
$node1->command_ok(
	[ 'pg_basebackup', '-D', $backup2path, '--no-sync', '-cfast',
	  '--incremental', $backup1path . '/backup_manifest' ],
	"incremental backup from node1");

# Result directory.
my $resultpath = $node1->backup_dir . '/result';

# Cause one file to be missing.
unlink("$backup2path/postgresql.conf")
	|| die "unlink $backup2path/postgresql.conf: $!";

# Test that it's detected.
$node1->command_fails_like(
	[ 'pg_combinebackup', $backup1path, $backup2path, '-o', $resultpath ],
	qr/error: "postgresql.conf" is present in the manifest but not on disk/,
	"can detect missing file");

# Now remove the backup_manifest so that we can't detect missing files.
unlink("$backup2path/backup_manifest")
	|| die "unlink $backup2path/backup_manifest: $!";

# Test that it's OK now. We have to use --no-manifest here because it's not
# possible to generate a manifest for the output directory without a manifest
# for the final input directory.
$node1->command_ok(
	[ 'pg_combinebackup', '--no-manifest', $backup1path, $backup2path,
		'-o', $resultpath ],
	"removing backup_manifest suppresses missing file detection");

# OK, that's all.
done_testing();
