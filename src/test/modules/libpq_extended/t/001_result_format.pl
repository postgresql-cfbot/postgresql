use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 4;
use Cwd;

my $node = get_new_node('main');
$node->init;
$node->start;

$ENV{PATH} = "$ENV{PATH}:" . getcwd();

# int2,int4,int8
$ENV{PGOPTIONS} = '-c result_format_auto_binary_types=21,23,20,20000';
$node->command_like([
	'test-result-format',
	$node->connstr('postgres'),
	"SELECT 1::int4, 2::int8, 'abc'::text",
	],
	qr/0->1 1->1 2->0/,
	"binary format is applied, nonexisting OID ignored");

$ENV{PGOPTIONS} = '-c result_format_auto_binary_types=foo,bar';
$node->command_fails([
	'test-result-format',
	$node->connstr('postgres'),
	"SELECT 1::int4, 2::int8, 'abc'::text",
	],
	"invalid setting is an error");

$node->stop('fast');
