--
-- FORMAT CASTS
--
-- A format cast registers a function for a (source type, target type) pair; a
-- CAST(... AS target FORMAT format_expr) is resolved through pg_format_cast and
-- calls that function.  This test covers both the CREATE/DROP FORMAT CAST
-- catalog DDL and the execution of formatted casts.

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

-- ====================================================================
-- Execution: a formatted cast resolves through pg_format_cast and calls the
-- registered format cast function.
-- ====================================================================

-- basic execution, using the (integer, text) format cast created above
SELECT CAST(5 AS text FORMAT 'abc');
-- the FORMAT expression may be any expression, coerced to text
SELECT CAST(5 AS text FORMAT 'a' || 'b');
SELECT CAST(5 AS text FORMAT 123);

-- The FORMAT expression is parse-analyzed independently of (and before) the
-- format cast lookup, so an invalid FORMAT expression reports a normal error.
SELECT CAST(5 AS text FORMAT no_such_column);

-- Like an ordinary cast that uses a cast function, a formatted cast checks
-- EXECUTE on the format cast function at use time.
REVOKE EXECUTE ON FUNCTION int4_to_text_fmt(integer, text) FROM PUBLIC;
CREATE ROLE regress_format_cast_noexec NOLOGIN;
SET ROLE regress_format_cast_noexec;
SELECT CAST(5 AS text FORMAT 'p');			-- fails: no EXECUTE on format cast function
RESET ROLE;
DROP ROLE regress_format_cast_noexec;
GRANT EXECUTE ON FUNCTION int4_to_text_fmt(integer, text) TO PUBLIC;

-- A FORMAT clause never falls back to an ordinary cast: text -> text is a
-- trivial ordinary cast, but a formatted cast still requires a format cast.
SELECT CAST('abc'::text AS text FORMAT 'whatever');
-- An unknown-type source literal is coerced to text first, so this also looks
-- up (text, text), not (unknown, text).
SELECT CAST('abc' AS text FORMAT 'fmt');
-- A missing format cast is an error even where an ordinary cast would be valid.
SELECT CAST(5 AS integer FORMAT 'x');

-- Define the (text, text) format cast and re-run the text-source cases.
CREATE FUNCTION text_to_text_fmt(text, text) RETURNS text
	LANGUAGE sql IMMUTABLE RETURN $1 || '/' || $2;
CREATE FORMAT CAST (text AS text)
	WITH FUNCTION text_to_text_fmt(text, text);
SELECT CAST('abc'::text AS text FORMAT 'whatever');
SELECT CAST('abc' AS text FORMAT 'fmt');

-- A type modifier on the target is enforced through the ordinary coercion path.
CREATE FUNCTION int4_to_vc_fmt(integer, text) RETURNS varchar
	LANGUAGE sql IMMUTABLE RETURN $1::text || $2;
CREATE FORMAT CAST (integer AS varchar)
	WITH FUNCTION int4_to_vc_fmt(integer, text);
SELECT CAST(5 AS varchar FORMAT 'XXXX');
SELECT CAST(5 AS varchar(3) FORMAT 'XXXX');			-- length 3 enforced

-- Domain target: the format cast must return the domain type, so the domain's
-- constraints are enforced by the function's result.
CREATE DOMAIN nonempty_text AS text CHECK (VALUE <> '');
CREATE FUNCTION text_to_netext_fmt(text, text) RETURNS nonempty_text
	LANGUAGE sql IMMUTABLE RETURN $2::nonempty_text;
CREATE FORMAT CAST (text AS nonempty_text)
	WITH FUNCTION text_to_netext_fmt(text, text);
SELECT CAST('z'::text AS nonempty_text FORMAT 'ok');
SELECT CAST('z'::text AS nonempty_text FORMAT '');	-- domain check violation

-- Drop the objects created in this execution section.
DROP FORMAT CAST (text AS text);
DROP FUNCTION text_to_text_fmt(text, text);
DROP FORMAT CAST (integer AS varchar);
DROP FUNCTION int4_to_vc_fmt(integer, text);
DROP FORMAT CAST (text AS nonempty_text);
DROP FUNCTION text_to_netext_fmt(text, text);
DROP DOMAIN nonempty_text;

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

-- ====================================================================
-- CoerceViaFormatCast: deparse fidelity and dependency on the format cast row
-- ====================================================================
CREATE FUNCTION i2t_view(integer, text) RETURNS text
	LANGUAGE sql IMMUTABLE RETURN $1::text || ':' || $2;
CREATE FORMAT CAST (integer AS text)
	WITH FUNCTION i2t_view(integer, text);

-- A view over a formatted cast deparses back to CAST(... FORMAT ...).
CREATE VIEW format_cast_view AS SELECT CAST(5 AS text FORMAT 'abc') AS x;
SELECT pg_get_viewdef('format_cast_view'::regclass, true);
SELECT * FROM format_cast_view;

-- The view depends on the pg_format_cast row, so DROP FORMAT CAST RESTRICT fails.
DROP FORMAT CAST (integer AS text);					-- fails (RESTRICT, view depends)
-- ... and CASCADE drops the dependent view.
DROP FORMAT CAST (integer AS text) CASCADE;
SELECT count(*) FROM pg_class WHERE relname = 'format_cast_view';
DROP FUNCTION i2t_view(integer, text);

-- ====================================================================
-- Collation: a formatted cast behaves like calling the format cast function
-- formatfunc(arg, format).  Its result collation is the result type's
-- collation, and the input collation is derived from the arg and FORMAT
-- expressions exactly as for an ordinary two-argument function call.
-- ====================================================================
-- text_larger(text, text) returns text and is collation-sensitive at the C
-- level (it reads PG_GET_COLLATION), so it exercises the input collation.
CREATE FORMAT CAST (text AS text)
	WITH FUNCTION pg_catalog.text_larger(text, text);
-- The default collation flows into the format cast call; if the input collation
-- were left unset this would fail with "could not determine which collation".
SELECT CAST('apple'::text AS text FORMAT 'banana'::text);
-- An explicit COLLATE on an operand is honored as the input collation.
SELECT CAST('apple'::text AS text FORMAT 'banana'::text COLLATE "C");
-- The result is collatable, so an explicit COLLATE on the cast itself works.
SELECT CAST('apple'::text AS text FORMAT 'banana'::text) COLLATE "C" < 'zzz';
-- Conflicting explicit input collations are rejected, just like a plain
-- function call text_larger('a' COLLATE "C", 'b' COLLATE "POSIX").
SELECT CAST('a'::text COLLATE "C" AS text FORMAT 'b'::text COLLATE "POSIX");
DROP FORMAT CAST (text AS text);

-- ====================================================================
-- ruleutils: arg and FORMAT subexpressions deparse unambiguously inside
-- CAST(...), needing no extra parentheses, and re-parse to the same thing.
-- ====================================================================
CREATE FUNCTION i2t_paren(integer, text) RETURNS text
	LANGUAGE sql IMMUTABLE RETURN $1::text || $2;
CREATE FORMAT CAST (integer AS text)
	WITH FUNCTION i2t_paren(integer, text);
CREATE VIEW format_cast_paren_view AS
	SELECT CAST((1 + 2) AS text FORMAT ('a' || 'b')) AS x;
SELECT pg_get_viewdef('format_cast_paren_view'::regclass, true);
SELECT * FROM format_cast_paren_view;
DROP VIEW format_cast_paren_view;
DROP FORMAT CAST (integer AS text);
DROP FUNCTION i2t_paren(integer, text);

-- ====================================================================
-- Dependency completeness: a view over a formatted cast depends (through the
-- pg_format_cast row) on the format cast function, so dropping the function with
-- CASCADE removes both the format cast row and the view in one step.
-- ====================================================================
CREATE FUNCTION i2t_chain(integer, text) RETURNS text
	LANGUAGE sql IMMUTABLE RETURN $1::text || $2;
CREATE FORMAT CAST (integer AS text)
	WITH FUNCTION i2t_chain(integer, text);
CREATE VIEW format_cast_chain_view AS SELECT CAST(7 AS text FORMAT 'q') AS x;
DROP FUNCTION i2t_chain(integer, text);					-- fails: format cast + view depend
DROP FUNCTION i2t_chain(integer, text) CASCADE;			-- drops format cast and view
SELECT count(*) FROM pg_format_cast
	WHERE fmtsource = 'integer'::regtype AND fmttarget = 'text'::regtype;
SELECT count(*) FROM pg_class WHERE relname = 'format_cast_chain_view';
