# Do basic sanity checks supported by pg_verify_checksums using
# an initialized cluster.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 42;

# Initialize node with checksums enabled.
my $node = get_new_node('node_checksum');
$node->init(extra => ['--data-checksums']);
my $pgdata = $node->data_dir;

# Control file should know that checksums are enabled.
command_like(['pg_controldata', $pgdata],
	     qr/Data page checksum version:.*1/,
		 'checksums enabled in control file');

# These are correct but empty files, so they should pass through.
append_to_file "$pgdata/global/99999", "";
append_to_file "$pgdata/global/99999.123", "";
append_to_file "$pgdata/global/99999_fsm", "";
append_to_file "$pgdata/global/99999_init", "";
append_to_file "$pgdata/global/99999_vm", "";
append_to_file "$pgdata/global/99999_init.123", "";
append_to_file "$pgdata/global/99999_fsm.123", "";
append_to_file "$pgdata/global/99999_vm.123", "";

# There are temporary files and folders with dummy contents, which
# should be ignored by the scan.
append_to_file "$pgdata/global/pgsql_tmp_123", "foo";
mkdir "$pgdata/global/pgsql_tmp";
append_to_file "$pgdata/global/pgsql_tmp/1.1", "foo";

# Checksums pass on a newly-created cluster
command_ok(['pg_verify_checksums',  '-D', $pgdata],
		   "succeeds with offline cluster");

# Checks cannot happen with an online cluster
$node->start;
command_fails(['pg_verify_checksums',  '-D', $pgdata],
			  "fails with online cluster");

# Create table to corrupt and get its relfilenode
my $relfilenode_corrupted = create_corruption($node, 'corrupt1', 'pg_default');

# Global checksum checks fail
$node->command_checks_all([ 'pg_verify_checksums', '-D', $pgdata],
						  1,
						  [qr/Bad checksums:.*1/],
						  [qr/checksum verification failed/],
						  'fails with corrupted data');

# Checksum checks on single relfilenode fail
$node->command_checks_all([ 'pg_verify_checksums', '-D', $pgdata, '-r',
							$relfilenode_corrupted],
						  1,
						  [qr/Bad checksums:.*1/],
						  [qr/checksum verification failed/],
						  'fails for corrupted data on single relfilenode');

# Drop corrupt table again and make sure there is no more corruption
$node->start;
$node->safe_psql('postgres', 'DROP TABLE corrupt1;');
$node->stop;
$node->command_ok(['pg_verify_checksums', '-D', $pgdata],
        'succeeds again: '.$node->data_dir);

# Create table to corrupt in a non-default tablespace and get its relfilenode
my $tablespace_dir = $node->data_dir."/../ts_corrupt_dir";
mkdir ($tablespace_dir);
$node->start;
$node->safe_psql('postgres', "CREATE TABLESPACE ts_corrupt LOCATION '".$tablespace_dir."';");
$relfilenode_corrupted = create_corruption($node, 'corrupt2', 'ts_corrupt');
$node->command_checks_all([ 'pg_verify_checksums', '-D', $pgdata],
						  1,
						  [qr/Bad checksums:.*1/],
						  [qr/checksum verification failed/],
						  'fails with corrupted data in non-default tablespace');

# Drop corrupt table again and make sure there is no more corruption
$node->start;
$node->safe_psql('postgres', 'DROP TABLE corrupt2;');
$node->stop;
$node->command_ok(['pg_verify_checksums', '-D', $pgdata],
        'succeeds again');

# Utility routine to create a table with corrupted checksums.
# It stops the node (if running), and starts it again.
sub create_corruption
{
	my $node = shift;
	my $table = shift;
	my $tablespace = shift;

	$node->safe_psql('postgres',
		"SELECT a INTO ".$table." FROM generate_series(1,10000) AS a;
		ALTER TABLE ".$table." SET (autovacuum_enabled=false);");

	$node->safe_psql('postgres',
		"ALTER TABLE ".$table." SET TABLESPACE ".$tablespace.";");

	my $file_corrupted = $node->safe_psql('postgres',
		"SELECT pg_relation_filepath('".$table."');");
	my $relfilenode_corrupted =  $node->safe_psql('postgres',
		"SELECT relfilenode FROM pg_class WHERE relname = '".$table."';");

	# Set page header and block size
	my $pageheader_size = 24;
	my $block_size = $node->safe_psql('postgres', 'SHOW block_size;');
	$node->stop;

	# Checksums are correct for single relfilenode as the table is not
	# corrupted yet.
	command_ok(['pg_verify_checksums',  '-D', $pgdata,
		'-r', $relfilenode_corrupted],
		"succeeds for single relfilenode with offline cluster");

	# Time to create some corruption
	open my $file, '+<', "$pgdata/$file_corrupted";
	seek($file, $pageheader_size, 0);
	syswrite($file, '\0\0\0\0\0\0\0\0\0');
	close $file;

	return $relfilenode_corrupted;
}

# Utility routine to check that pg_verify_checksums is able to detect
# correctly-named relation files filled with some corrupted data.
sub fail_corrupt
{
	my $node = shift;
	my $file = shift;
	my $pgdata = $node->data_dir;

	# Create the file with some dummy data in it.
	my $file_name = "$pgdata/global/$file";
	append_to_file $file_name, "foo";

	$node->command_checks_all([ 'pg_verify_checksums', '-D', $pgdata],
						  1,
						  [qr/^$/],
						  [qr/could not read block 0 in file.*$file\":/],
						  "fails for corrupted data in $file");

	# Remove file to prevent future lookup errors on conflicts.
	unlink $file_name;
	return;
}

# Authorized relation files filled with corrupted data cause the
# checksum checks to fail.  Make sure to use file names different
# than the previous ones.
fail_corrupt($node, "99990");
fail_corrupt($node, "99990.123");
fail_corrupt($node, "99990_fsm");
fail_corrupt($node, "99990_init");
fail_corrupt($node, "99990_vm");
fail_corrupt($node, "99990_init.123");
fail_corrupt($node, "99990_fsm.123");
fail_corrupt($node, "99990_vm.123");
