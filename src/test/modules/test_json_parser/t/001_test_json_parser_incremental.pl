
use strict;
use warnings;

use PostgreSQL::Test::Utils;
use Test::More;
use FindBin;

my $test_file = "$FindBin::RealBin/../tiny.json";

my $exe = "$ENV{TESTDATADIR}/../test_json_parser_incremental";

my ($stdout, $stderr) = run_command( [$exe, $test_file] );

ok($stdout =~ /SUCCESS/, "test succeeds");
ok(!$stderr, "no error output");


done_testing();





