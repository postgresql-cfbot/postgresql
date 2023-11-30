CREATE EXTENSION bytea_plperlu CASCADE;

CREATE FUNCTION cat_bytea(bytea) RETURNS bytea LANGUAGE plperlu
 TRANSFORM FOR TYPE bytea
 AS $$
    return $_[0];
 $$;

SELECT data = cat_bytea(data)
    FROM (
        SELECT decode(repeat(unnest(ARRAY[ 'a','abc', 'abcd', 'abcdefgh\000ijkl12' , '爱' ]), 10000), 'escape') data
    ) line;

CREATE FUNCTION perlu_inverse_bytes(bytea) RETURNS bytea
TRANSFORM FOR TYPE bytea
AS $$
	return join '', reverse split('', $_[0]);
$$ LANGUAGE plperlu;

SELECT 'ξενία'::bytea, perlu_inverse_bytes('ξενία'::bytea);

DROP EXTENSION plperlu CASCADE;
