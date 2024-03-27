CREATE EXTENSION bytea_plperl CASCADE;

CREATE FUNCTION cat_bytea(bytea) RETURNS bytea LANGUAGE plperl
 TRANSFORM FOR TYPE bytea
 AS $$
    return $_[0];
 $$;

SELECT data = cat_bytea(data)
    FROM (
        SELECT decode(repeat(unnest(ARRAY[ 'a','abc', 'abcd', 'abcdefgh\000ijkl12' , '爱' ]), 10000), 'escape') data
    ) line;

CREATE FUNCTION perl_inverse_bytes(bytea) RETURNS bytea
TRANSFORM FOR TYPE bytea
AS $$
	return join '', reverse split('', $_[0]);
$$ LANGUAGE plperl;

SELECT 'ξενία'::bytea, perl_inverse_bytes('ξενία'::bytea);

CREATE FUNCTION plperl_bytea_overload() RETURNS bytea LANGUAGE plperl
 TRANSFORM FOR TYPE bytea
 AS $$
   package StringOverload { use overload '""' => sub { "stuff" }; }
   return bless {}, "StringOverload";
 $$;

SELECT encode(plperl_bytea_overload(), 'escape') string;

DROP EXTENSION plperl CASCADE;
