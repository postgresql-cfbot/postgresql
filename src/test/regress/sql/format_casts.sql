--
-- FORMAT CASTS
--
-- CREATE/DROP FORMAT CAST registers format cast metadata in pg_format_cast,
-- keyed by a (source type, target type) pair.  This is catalog and DDL
-- infrastructure only; it does not transform or execute formatted casts.

-- A simple format cast function with the required signature
--   function(source_type, text) returns target_type
CREATE FUNCTION int4_to_text_fmt(integer, text) RETURNS text
	LANGUAGE sql IMMUTABLE RETURN $1::text || ':' || $2;

CREATE FORMAT CAST (integer AS text)
	WITH FUNCTION int4_to_text_fmt(integer, text);

-- It shows up in the catalog (use regtype/regprocedure, not raw OIDs)
SELECT fmtsource::regtype, fmttarget::regtype, fmtfunc::regprocedure
	FROM pg_format_cast
	WHERE fmtsource = 'integer'::regtype AND fmttarget = 'text'::regtype;

-- ... and as a first-class object
SELECT pg_describe_object('pg_format_cast'::regclass, oid, 0)
	FROM pg_format_cast
	WHERE fmtsource = 'integer'::regtype AND fmttarget = 'text'::regtype;

-- Object identity and COMMENT use "FORMAT CAST (source AS target)" (no "FOR CAST").
SELECT identity FROM pg_identify_object('pg_format_cast'::regclass,
	(SELECT oid FROM pg_format_cast
	 WHERE fmtsource = 'integer'::regtype AND fmttarget = 'text'::regtype), 0);
COMMENT ON FORMAT CAST (integer AS text) IS 'demo format cast';
SELECT obj_description((SELECT oid FROM pg_format_cast
	WHERE fmtsource = 'integer'::regtype AND fmttarget = 'text'::regtype),
	'pg_format_cast');
COMMENT ON FORMAT CAST (integer AS text) IS NULL;

-- pg_identify_object_as_address() must round-trip back through
-- pg_get_object_address() to the same catalog object.
WITH fmt AS (
	SELECT oid FROM pg_format_cast
	WHERE fmtsource = 'integer'::regtype AND fmttarget = 'text'::regtype
), addr AS (
	SELECT * FROM pg_identify_object_as_address('pg_format_cast'::regclass,
												(SELECT oid FROM fmt), 0)
)
SELECT addr.type, addr.object_names, addr.object_args,
	   (SELECT classid = 'pg_format_cast'::regclass
			   AND objid = (SELECT oid FROM fmt)
			   AND objsubid = 0
		FROM pg_get_object_address(addr.type, addr.object_names, addr.object_args))
	   AS round_trips
	FROM addr;

-- Only one format cast per (source, target) pair
CREATE FORMAT CAST (integer AS text)
	WITH FUNCTION int4_to_text_fmt(integer, text);

-- Function signature validation
CREATE FUNCTION fmt_bad_nargs(integer) RETURNS text
	LANGUAGE sql IMMUTABLE RETURN $1::text;
CREATE FORMAT CAST (integer AS text)
	WITH FUNCTION fmt_bad_nargs(integer);			-- wrong # of arguments

CREATE FUNCTION fmt_bad_arg2(integer, integer) RETURNS text
	LANGUAGE sql IMMUTABLE RETURN $1::text;
CREATE FORMAT CAST (integer AS text)
	WITH FUNCTION fmt_bad_arg2(integer, integer);	-- second arg not text

CREATE FUNCTION fmt_bad_arg1(bigint, text) RETURNS text
	LANGUAGE sql IMMUTABLE RETURN $1::text;
CREATE FORMAT CAST (integer AS text)
	WITH FUNCTION fmt_bad_arg1(bigint, text);		-- first arg mismatch

CREATE FUNCTION fmt_bad_ret(integer, text) RETURNS boolean
	LANGUAGE sql IMMUTABLE RETURN true;
CREATE FORMAT CAST (integer AS text)
	WITH FUNCTION fmt_bad_ret(integer, text);		-- return type mismatch

CREATE FUNCTION fmt_bad_set(integer, text) RETURNS SETOF text
	LANGUAGE sql IMMUTABLE AS $$ SELECT $1::text $$;
CREATE FORMAT CAST (integer AS text)
	WITH FUNCTION fmt_bad_set(integer, text);		-- set-returning rejected

-- No pseudo-types
CREATE FUNCTION fmt_anyel(anyelement, text) RETURNS text
	LANGUAGE sql IMMUTABLE AS $$ SELECT $2 $$;
CREATE FORMAT CAST (anyelement AS text)
	WITH FUNCTION fmt_anyel(anyelement, text);

-- Registering a format cast does not enable a formatted cast: the FORMAT
-- clause must not be silently ignored or rewritten to a built-in function,
-- so CAST(... FORMAT ...) is rejected during parse analysis.
SELECT CAST(5 AS text FORMAT 'YYYY');

-- Dependency behavior: the format cast depends on its function.
DROP FUNCTION int4_to_text_fmt(integer, text);				-- fails (RESTRICT)
DROP FUNCTION int4_to_text_fmt(integer, text) CASCADE;		-- drops format cast
SELECT count(*) FROM pg_format_cast
	WHERE fmtsource = 'integer'::regtype AND fmttarget = 'text'::regtype;

-- Privileges: the creator must own the source or the target type.  (The
-- ownership check happens before the function is looked up, so the function
-- name here is irrelevant.)
CREATE ROLE regress_format_cast_user;
SET ROLE regress_format_cast_user;
CREATE FORMAT CAST (text AS integer)
	WITH FUNCTION pg_catalog.length(text);
RESET ROLE;
DROP ROLE regress_format_cast_user;

-- DROP FORMAT CAST, including IF EXISTS
CREATE FUNCTION int4_to_text_fmt(integer, text) RETURNS text
	LANGUAGE sql IMMUTABLE RETURN $1::text || ':' || $2;
CREATE FORMAT CAST (integer AS text)
	WITH FUNCTION int4_to_text_fmt(integer, text);
DROP FORMAT CAST (integer AS text);
DROP FORMAT CAST (integer AS text);					-- fails, gone
DROP FORMAT CAST IF EXISTS (integer AS text);		-- notice, no error

-- Clean up
DROP FUNCTION int4_to_text_fmt(integer, text);
DROP FUNCTION fmt_bad_nargs(integer);
DROP FUNCTION fmt_bad_arg2(integer, integer);
DROP FUNCTION fmt_bad_arg1(bigint, text);
DROP FUNCTION fmt_bad_ret(integer, text);
DROP FUNCTION fmt_bad_set(integer, text);
DROP FUNCTION fmt_anyel(anyelement, text);
