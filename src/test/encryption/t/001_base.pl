use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 3;

my $keyword = "secret keyword";
my $node = get_new_node('test');
$node->init(enable_encryption => 1);
$node->start;

# Check is the given relation file is encrypted
sub is_encrypted
{
	my $node = shift;
	my $filepath = shift;
	my $expected = shift;
	my $testname = shift;
	my $pgdata = $node->data_dir;

	open my $file, '<' , "$pgdata/$filepath";
	sysread $file, my $buffer, 8192;

	my $ret = $buffer !~ /$keyword/ ? 1 : 0;

	is($ret, $expected, $testname);

	close $file;
}

$node->safe_psql('postgres',
				 qq(
				 CREATE TABLE test (a text);
				 INSERT INTO test VALUES ('$keyword');
				 ));
my $table_filepath = $node->safe_psql('postgres', qq(SELECT pg_relation_filepath('test')));
my $wal_filepath = 'pg_wal' . $node->safe_psql('postgres', qq(SELECT pg_walfile_name(pg_current_wal_lsn())));

# Read encrypted table
my $ret = $node->safe_psql('postgres', 'SELECT a FROM test');
is($ret, "$keyword", 'Read encrypted table');

# Sync to disk
$node->safe_psql('postgres', 'CHECKPOINT');

# Encrypted table must be encrypted
is_encrypted($node, $table_filepath, 1, 'table is encrypted');
is_encrypted($node, $wal_filepath, 1, 'WAL is encrypted');
