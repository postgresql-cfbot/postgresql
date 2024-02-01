# Run ecpg regression tests in a loop
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Basename;

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('p1');
$node_primary->init(
	'auth_extra' => [ '--create-role', 'regress_ecpg_user1,regress_ecpg_user2' ]
);

$node_primary->start;

my $outputdir = $PostgreSQL::Test::Utils::tmp_check;
my $NUM_ITERATIONS = 2;

for (my $i = 0; $i < $NUM_ITERATIONS; $i++)
{
my $extra_opts = $ENV{EXTRA_REGRESS_OPTS} || "";

my $rc =
  system("\"$outputdir/../../../../src/interfaces/ecpg/test/pg_regress_ecpg\" "
	  . " $extra_opts "
	  . "--dbname=ecpg1_regression,ecpg2_regression --create-role=regress_ecpg_user1,regress_ecpg_user2 "
	  . "--bindir= "
	  . "--host="
	  . $node_primary->host . " "
	  . "--port="
	  . $node_primary->port . " "
	  . "--outputdir=$outputdir "
	  . "--inputdir=$outputdir/../../../../src/interfaces/ecpg/test/ "
	  . "--expecteddir=../../../../src/interfaces/ecpg/test/ "
	  . ( "connect/test5 " x 50)
);

if ($rc != 0)
{
	# Dump out the regression diffs file, if there is one
	my $diffs = "$outputdir/regression.diffs";
	if (-e $diffs)
	{
		print "=== dumping $diffs ===\n";
		print slurp_file($diffs);
		print "=== EOF ===\n";
	}
}
is($rc, 0, 'ecpg regression tests pass');
($rc == 0) || last;
}

$node_primary->stop;

done_testing();
