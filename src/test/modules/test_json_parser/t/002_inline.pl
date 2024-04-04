use strict;
use warnings;

use PostgreSQL::Test::Utils;
use Test::More;

use File::Temp qw(tempfile);

sub test
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($name, $json, %params) = @_;
	my $exe = "test_json_parser_incremental";
	my $chunk = length($json);

	if ($chunk > 64)
	{
		$chunk = 64;
	}

	my ($fh, $fname) = tempfile(UNLINK => 1);
	print $fh "$json";
	close($fh);

	foreach my $size (reverse(1..$chunk))
	{
		my ($stdout, $stderr) = run_command( [$exe, "-c", $size, $fname] );

		if (defined($params{error}))
		{
			unlike($stdout, qr/SUCCESS/, "$name, chunk size $size: test fails");
			like($stderr, $params{error}, "$name, chunk size $size: correct error output");
		}
		else
		{
			like($stdout, qr/SUCCESS/, "$name, chunk size $size: test succeeds");
			is($stderr, "", "$name, chunk size $size: no error output");
		}
	}
}

test("number", "12345");
test("string", '"hello"');
test("false", "false");
test("true", "true");
test("null", "null");
test("empty object", "{}");
test("empty array", "[]");
test("array with number", "[12345]");
test("array with numbers", "[12345,67890]");
test("array with null", "[null]");
test("array with string", '["hello"]');
test("array with boolean", '[false]');
test("single pair", '{"key": "value"}');
test("heavily nested array", "[" x 3200 . "]" x 3200);
test("serial escapes", '"\\\\\\\\\\\\\\\\"');
test("interrupted escapes", '"\\\\\\"\\\\\\\\\\"\\\\"');
test("whitespace", '     ""     ');

test("unclosed empty object", "{", error => qr/input string ended unexpectedly/);
test("bad key", "{{", error => qr/Expected string or "}", but found "\{"/);
test("bad key", "{{}", error => qr/Expected string or "}", but found "\{"/);
test("numeric key", "{1234: 2}", error => qr/Expected string or "}", but found "1234"/);
test("second numeric key", '{"a": "a", 1234: 2}', error => qr/Expected string, but found "1234"/);
test("unclosed object with pair", '{"key": "value"', error => qr/input string ended unexpectedly/);
test("missing key value", '{"key": }', error => qr/Expected JSON value, but found "}"/);
test("missing colon", '{"key" 12345}', error => qr/Expected ":", but found "12345"/);
test("missing comma", '{"key": 12345 12345}', error => qr/Expected "," or "}", but found "12345"/);
test("overnested array", "[" x 6401, error => qr/maximum permitted depth is 6400/);
test("overclosed array", "[]]", error => qr/Expected end of input, but found "]"/);
test("unexpected token in array", "[ }}} ]", error => qr/Expected array element or "]", but found "}"/);
test("junk punctuation", "[ ||| ]", error => qr/Token "|" is invalid/);
test("missing comma in array", "[123 123]", error => qr/Expected "," or "]", but found "123"/);
test("misspelled boolean", "tru", error => qr/Token "tru" is invalid/);
test("misspelled boolean in array", "[tru]", error => qr/Token "tru" is invalid/);
test("smashed top-level scalar", "12zz", error => qr/Token "12zz" is invalid/);
test("smashed scalar in array", "[12zz]", error => qr/Token "12zz" is invalid/);
test("unknown escape sequence", '"hello\vworld"', error => qr/Escape sequence "\\v" is invalid/);
test("unescaped control", "\"hello\tworld\"", error => qr/Character with value 0x09 must be escaped/);
test("incorrect escape count", '"\\\\\\\\\\\\\\"', error => qr/Token ""\\\\\\\\\\\\\\"" is invalid/);

done_testing();
