CREATE EXTENSION jsonb_plperlu CASCADE;

-- test hash -> jsonb
CREATE FUNCTION testHVToJsonb() RETURNS jsonb
LANGUAGE plperlu
TRANSFORM FOR TYPE jsonb
AS $$
$val = {a => 1, b => 'boo', c => undef};
return $val;
$$;

SELECT testHVToJsonb();

-- test array -> jsonb
CREATE FUNCTION testAVToJsonb() RETURNS jsonb
LANGUAGE plperlu
TRANSFORM FOR TYPE jsonb
AS $$
$val = [{a => 1, b => 'boo', c => undef}, {d => 2}];
return $val;
$$;

SELECT testAVToJsonb();

-- test scalar -> jsonb
CREATE FUNCTION testSVToJsonb() RETURNS jsonb
LANGUAGE plperlu
TRANSFORM FOR TYPE jsonb
AS $$
$val = 1;
return $val;
$$;

SELECT testSVToJsonb();

-- test blessed scalar -> jsonb
CREATE FUNCTION testBlessedToJsonb() RETURNS jsonb
LANGUAGE plperlu
TRANSFORM FOR TYPE jsonb
AS $$
my $class = shift;
my $tmp = { a=>"a", 1=>"1" };
bless $tmp, $class;
return $tmp;
$$;

SELECT testBlessedToJsonb();

-- test blessed scalar -> jsonb
CREATE FUNCTION testRegexpToJsonb() RETURNS jsonb
LANGUAGE plperlu
TRANSFORM FOR TYPE jsonb
AS $$
return ('1' =~ m(0\t2));
$$;

SELECT testRegexpToJsonb();


-- test jsonb -> scalar -> jsonb
CREATE FUNCTION testSVToJsonb2(val jsonb) RETURNS jsonb
LANGUAGE plperlu
TRANSFORM FOR TYPE jsonb
AS $$
return $_[0];
$$;


SELECT testSVToJsonb2('null');
SELECT testSVToJsonb2('1');
SELECT testSVToJsonb2('1E+131071');
SELECT testSVToJsonb2('-1');
SELECT testSVToJsonb2('1.2');
SELECT testSVToJsonb2('-1.2');
SELECT testSVToJsonb2('"string"');
SELECT testSVToJsonb2('"NaN"');

SELECT testSVToJsonb2('true');
SELECT testSVToJsonb2('false');

SELECT testSVToJsonb2('[]');
SELECT testSVToJsonb2('[null,null]');
SELECT testSVToJsonb2('[1,2,3]');
SELECT testSVToJsonb2('[-1,2,-3]');
SELECT testSVToJsonb2('[1.2,2.3,3.4]');
SELECT testSVToJsonb2('[-1.2,2.3,-3.4]');
SELECT testSVToJsonb2('["string1","string2"]');

SELECT testSVToJsonb2('{}');
SELECT testSVToJsonb2('{"1":null}');
SELECT testSVToJsonb2('{"1":1}');
SELECT testSVToJsonb2('{"1":-1}');
SELECT testSVToJsonb2('{"1":1.1}');
SELECT testSVToJsonb2('{"1":-1.1}');
SELECT testSVToJsonb2('{"1":"string1"}');

SELECT testSVToJsonb2('{"1":{"2":[3,4,5]},"2":3}');

-- testing large numbers which are not represented as "inf" inside perl.
-- 1E+309 - is inf while 1E+308 is not
SELECT testSVToJsonb2('1E+308');


DROP EXTENSION plperlu CASCADE;
