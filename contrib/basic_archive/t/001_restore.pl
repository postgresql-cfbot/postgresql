
# Copyright (c) 2023, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# start a node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init(has_archiving => 1, allows_streaming => 1);
my $archive_dir = $node->archive_dir;
$archive_dir =~ s!\\!/!g if $PostgreSQL::Test::Utils::windows_os;
$node->append_conf('postgresql.conf', "archive_command = ''");
$node->append_conf('postgresql.conf', "archive_library = 'basic_archive'");
$node->append_conf('postgresql.conf', "basic_archive.archive_directory = '$archive_dir'");
$node->start;

# backup the node
my $backup = 'backup';
$node->backup($backup);

# generate some new WAL files
$node->safe_psql('postgres', "CREATE TABLE test (a INT);");
$node->safe_psql('postgres', "SELECT pg_switch_wal();");
$node->safe_psql('postgres', "INSERT INTO test VALUES (1);");

# shut down the node (this should archive all WAL files)
$node->stop;

# restore from the backup
my $restore = PostgreSQL::Test::Cluster->new('restore');
$restore->init_from_backup($node, $backup, has_restoring => 1, standby => 0);
$restore->append_conf('postgresql.conf', "restore_command = ''");
$restore->append_conf('postgresql.conf', "restore_library = 'basic_archive'");
$restore->append_conf('postgresql.conf', "basic_archive.archive_directory = '$archive_dir'");
$restore->start;

# ensure post-backup WAL is replayed
my $result = $restore->poll_query_until("postgres", "SELECT count(*) = 1 FROM test;");
is($result, "1", "check restore content");

done_testing();
