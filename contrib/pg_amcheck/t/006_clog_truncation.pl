# This regression test checks the behavior of the heap validation in the
# presence of clog corruption.

use strict;
use warnings;

use PostgresNode;
use TestLib;

use Test::More tests => 3;

my ($node, $pgdata, $clogdir);

sub count_clog_files
{
	my $result = 0;
	opendir(DIR, $clogdir) or die "Cannot opendir $clogdir: $!";
	while (my $fname = readdir(DIR))
	{
		$result++ if (-f "$clogdir/$fname");
	}
	closedir(DIR);
	return $result;
}

# Burn through enough xids that at least three clog files exists in pg_xact/
sub create_three_clog_files
{
	print STDERR "Generating clog entries....\n";

	$node->safe_psql('postgres', q(
		CREATE PROCEDURE burn_xids ()
		LANGUAGE plpgsql
		AS $$
		DECLARE
			loopcnt BIGINT;
		BEGIN
			FOR loopcnt IN 1..32768
			LOOP
				PERFORM txid_current();
				COMMIT;
			END LOOP;
		END;
		$$;
	));

	do {
		$node->safe_psql('postgres', 'INSERT INTO test_0 (i) VALUES (0)');
		$node->safe_psql('postgres', 'CALL burn_xids()');
		print STDERR "Burned transaction ids...\n";
		$node->safe_psql('postgres', 'INSERT INTO test_1 (i) VALUES (1)');
	} while (count_clog_files() < 3);
}

# Of the clog files in pg_xact, remove the second one, sorted by name order.
# This function, used along with create_three_clog_files(), is intended to
# remove neither the newest nor the oldest clog file.  Experimentation shows
# that removing the newest clog file works ok, but for future-proofing, remove
# one less likely to be checked at server startup.
sub unlink_second_clog_file
{
	my @paths;
	opendir(DIR, $clogdir) or die "Cannot opendir $clogdir: $!";
	while (my $fname = readdir(DIR))
	{
		my $path = "$clogdir/$fname";
		next unless -f $path;
		push @paths, $path;
	}
	closedir(DIR);

	my @ordered = sort { $a cmp $b } @paths;
	unlink $ordered[1];
}

# Set umask so test directories and files are created with default permissions
umask(0077);

# Set up the node.  Once we corrupt clog, autovacuum workers visiting tables
# could crash the backend.  Disable autovacuum so that won't happen.
$node = get_new_node('test');
$node->init;
$node->append_conf('postgresql.conf', 'autovacuum=off');
$pgdata = $node->data_dir;
$clogdir = join('/', $pgdata, 'pg_xact');
$node->start;
$node->safe_psql('postgres', "CREATE EXTENSION amcheck");
$node->safe_psql('postgres', "CREATE TABLE test_0 (i INTEGER)");
$node->safe_psql('postgres', "CREATE TABLE test_1 (i INTEGER)");
$node->safe_psql('postgres', "VACUUM FREEZE");

create_three_clog_files();

# Corruptly delete a clog file
$node->stop;
unlink_second_clog_file();
$node->start;

my $port = $node->port;

# Run pg_amcheck against the corrupt database, looking for clog related
# corruption messages
$node->command_checks_all(
	['pg_amcheck', '--check-toast', '--skip-indexes', '-p', $port, 'postgres'],
	0,
	[ qr/transaction status is lost/ ],
	[ qr/^$/ ],
	'Expected corruption message output');

$node->teardown_node;
$node->clean_node;
