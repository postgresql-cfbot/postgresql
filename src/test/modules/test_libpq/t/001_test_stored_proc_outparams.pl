use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More;

my $node = get_new_node('main');
$node->init;
$node->start;

my @creation = (
	q{
       create procedure out_params_d(arg int, out textres text, out intres int)
        language plpgsql as
       $$
       begin
          textres := 'foo';
          intres := arg * 5;
       end;
       $$},
	q{
       create procedure out_params_d(arg int, out boolres boolean,
                                     out floatres float8)
       language plpgsql as
       $$
       begin
          boolres := false;
          floatres := arg * 8.0;
       end;
       $$},

	q{
       create procedure out_params_uniq(arg int, out bres boolean,
                                        out fres float8)
       language plpgsql as
       $$
       begin
          bres := true;
          fres := arg * 13.0;
       end;
       $$},);



$node->safe_psql('postgres', $_) foreach (@creation);

my $connstr = $node->connstr('postgres');

# query, test name, status, match pattern
my @tests = (
	[
		'call out_params_uniq($1,$2,$3)',
		'unique - out param casts not needed',
		'ok',
		qr{bres\|fres.*t\s*\|\s*26}s
	],
	[
		'call out_params_d($1,$2::text,$3)',
		'duplicate - text param',
		'ok',
		qr{textres\|intres.*foo\s*\|\s*10}s
	],
	[
		'call out_params_d($1,$2::boolean,$3)',
		'duplicate - boolean param',
		'ok',
		qr{boolres\|floatres.*f\s*\|\s*16}s
	],
	[
		'call no_such_proc($1,$2,$3)',
		'no such proc',
		'bad',
		qr{ERROR:\s+procedure no_such_proc\(integer, unknown, unknown\) does not exist}
	],
	[
		'call out_params_d($1,$2,$3)',
		'duplicate - not enough casts',
		'bad',
		qr{ERROR:\s+procedure out_params_d\(integer, unknown, unknown\) is not unique}
	],);

# TAP tests are run from the source direectory, but the binary is found in
# the build directory. This is the simplest way to get at it

my $exe = "$ENV{TESTDIR}/test_stored_proc_outparams";

foreach my $test (@tests)
{
	my ($query, $name, $type, $match) = @$test;
	if ($type eq 'ok')
	{
		TestLib::command_like([ $exe, $connstr, $query ], $match, $name);
	}
	else
	{
		TestLib::command_fails_like([ $exe, $connstr, $query ], $match,
			$name);
	}
}

done_testing();

$node->stop('fast');

