# Copyright (c) 2024-2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

my ($out, $err) = run_command(['libpq_protocol_cursor', 'tests']);
die "oops: $err" unless $err eq '';
my @tests = split(/\s+/, $out);

for my $testname (@tests)
{
	# cursor_options_without_extension must run without protocol_cursor enabled
	my $connstr = $node->connstr('postgres');
	if ($testname eq 'cursor_options_without_extension')
	{
		$connstr .= " protocol_cursor=0";
	}
	else
	{
		$connstr .= " protocol_cursor=1 max_protocol_version=latest";
	}

	$node->command_ok(
		[
			'libpq_protocol_cursor',
			$testname,
			$connstr
		],
		"libpq_protocol_cursor $testname");
}

$node->stop('fast');

done_testing();
