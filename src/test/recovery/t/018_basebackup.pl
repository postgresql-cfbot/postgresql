# Test basebackup worker functionality
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;

my $node1 = get_new_node('node1');
$node1->init(allows_streaming => 1);
$node1->start;

$node1->safe_psql('postgres',
				  "CREATE TABLE tab_int AS SELECT generate_series(1,1000) AS a");

my $node2 = get_new_node('node2');
$node2->init(allows_streaming => 1, extra => [ '--replica' ]);
$node2->append_conf('postgresql.conf', "primary_conninfo = '" . $node1->connstr . "'");
my $old_mtime = (stat($node2->data_dir . '/postgresql.conf'))[9];
$node2->start;

$node1->wait_for_catchup($node2, 'replay', $node1->lsn('insert'));

is($node2->safe_psql('postgres', "SELECT count(*) FROM tab_int"),
   qq(1000),
   'check content of standby');

my $new_mtime = (stat($node2->data_dir . '/postgresql.conf'))[9];
is($new_mtime, $old_mtime,
   'configuration files were not copied');
