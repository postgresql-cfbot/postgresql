# Test collations, in particular nondeterministic ones
# (only works with ICU)
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

if ($ENV{with_icu} eq 'yes')
{
	plan tests => 2;
}
else
{
	plan skip_all => 'ICU not supported by this build';
}

my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

$node_subscriber->safe_psql('postgres',
   "CREATE COLLATION case_insensitive (provider = icu, locale = 'und-u-ks-level2', deterministic = false)");

# table with replica identity index

$node_publisher->safe_psql('postgres',
   "CREATE TABLE tab1 (a text PRIMARY KEY, b text)");

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab1 VALUES ('abc', 'abc'), ('def', 'def')");

$node_subscriber->safe_psql('postgres',
   "CREATE TABLE tab1 (a text COLLATE case_insensitive PRIMARY KEY, b text)");

$node_subscriber->safe_psql('postgres',
	"INSERT INTO tab1 VALUES ('ABC', 'abc'), ('GHI', 'ghi')");

# table with replica identity full

$node_publisher->safe_psql('postgres',
   "CREATE TABLE tab2 (a text, b text)");
$node_publisher->safe_psql('postgres',
   "ALTER TABLE tab2 REPLICA IDENTITY FULL");

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab2 VALUES ('abc', 'abc'), ('def', 'def')");

$node_subscriber->safe_psql('postgres',
   "CREATE TABLE tab2 (a text COLLATE case_insensitive, b text)");
$node_subscriber->safe_psql('postgres',
   "ALTER TABLE tab2 REPLICA IDENTITY FULL");

$node_subscriber->safe_psql('postgres',
	"INSERT INTO tab2 VALUES ('ABC', 'abc'), ('GHI', 'ghi')");

# set up publication, subscription

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub1 FOR ALL TABLES");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub1 CONNECTION '$publisher_connstr application_name=sub1' PUBLICATION pub1 WITH (copy_data = false)");

$node_publisher->wait_for_catchup('sub1');

# test with replica identity index

$node_publisher->safe_psql('postgres',
	"UPDATE tab1 SET b = 'xyz' WHERE b = 'abc'");

$node_publisher->wait_for_catchup('sub1');

# Note: Even though the UPDATE command above only updates column "b",
# the replication target will also update column "a", because the
# whole row is shippped.
is($node_subscriber->safe_psql('postgres', "SELECT a, b FROM tab1 ORDER BY a"),
   qq(abc|xyz
GHI|ghi),
  'update of case insensitive primary key');

# test with replica identity full

$node_publisher->safe_psql('postgres',
	"UPDATE tab2 SET b = 'xyz' WHERE b = 'abc'");

$node_publisher->wait_for_catchup('sub1');

is($node_subscriber->safe_psql('postgres', "SELECT a, b FROM tab2 ORDER BY a"),
   qq(abc|xyz
GHI|ghi),
  'update of case insensitive column with replica identity full');
