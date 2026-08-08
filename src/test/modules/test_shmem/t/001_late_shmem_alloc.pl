# Copyright (c) 2025-2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

###
# Test allocating memory after startup, i.e. when the library is not
# in shared_preload_libraries
###
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;


$node->safe_psql("postgres", "CREATE EXTENSION test_shmem;");

# Check that the attach counter is incremented on a new connection
my $attach_count1 =
  $node->safe_psql("postgres", "SELECT get_test_shmem_attach_count();");
my $attach_count2 =
  $node->safe_psql("postgres", "SELECT get_test_shmem_attach_count();");
cmp_ok($attach_count2, '>', $attach_count1,
	"attach callback is called in each backend");

sub try_shmem_failure_twice
{
	my ($mode) = @_;
	my $sql = qq[
DO \$\$
BEGIN
	FOR i IN 1..2 LOOP
		BEGIN
			PERFORM test_shmem_failure($mode);
		EXCEPTION WHEN others THEN
			RAISE NOTICE 'attempt %: %', i, SQLERRM;
		END;
	END LOOP;
END
\$\$;];
	return $node->psql('postgres', $sql);
}

# The state is backend-local, so the two attempts must share a session.
foreach my $mode (0, 1)
{
	my ($ret, $stdout, $stderr) = try_shmem_failure_twice($mode);

	is($ret, 0, "session survives repeated failing shmem request $mode");
	like($stderr, qr/attempt 1: /, "shmem request $mode fails");
	like($stderr, qr/attempt 2: /, "shmem request $mode fails when retried");
}

my ($ret, $stdout, $stderr) = try_shmem_failure_twice(2);
is($ret, 0, 'session survives a partly oversized request batch');
like($stderr, qr/attempt 2: .*not enough shared memory/,
	'a partly oversized batch can be retried');
unlike($stderr, qr/already been initialized/,
	'a partly oversized batch does not wedge later attempts');
is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_shmem_allocations WHERE name LIKE 'test_shmem partial%';"
	),
	'0',
	'a partly oversized batch creates no areas');

my ($legacy_ret, $legacy_out, $legacy_err) =
  $node->psql('postgres', 'SELECT test_shmem_failure(3);');
isnt($legacy_ret, 0, 'legacy allocation from an init callback is rejected');
like($legacy_err, qr/cannot call ShmemInitStruct\(\) while holding ShmemIndexLock/,
	'legacy allocation reports a clear error instead of hanging');
$node->stop;

###
# Test that loading via shared_preload_libraries also works
###
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'test_shmem'");
$node->start;

# When loaded via shared_preload_libraries, the attach callback is
# called or not, depending on whether this is an EXEC_BACKEND build.
my $exec_backend =
  $node->safe_psql("postgres", "SHOW debug_exec_backend;") eq 'on';
$attach_count1 =
  $node->safe_psql("postgres", "SELECT get_test_shmem_attach_count();");
$attach_count2 =
  $node->safe_psql("postgres", "SELECT get_test_shmem_attach_count();");

if ($exec_backend)
{
	cmp_ok($attach_count2, '>', $attach_count1,
		"attach callback is called in each backend when loaded via shared_preload_libraries"
	);
}
else
{
	ok( $attach_count1 == 0 && $attach_count2 == 0,
		"attach callback is not called when loaded via shared_preload_libraries"
	);
}

$node->stop;
done_testing();
