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

-- No pseudo-types: a format cast's source/target may not be a pseudo-type, so
-- CREATE FORMAT CAST on a polymorphic (anyelement) source is intentionally
-- rejected.
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

-- ====================================================================
-- Built-in datetime/string format casts.
-- These rows are registered in pg_format_cast and reuse PostgreSQL's
-- existing formatting template behavior.
-- This initial set covers date, timestamp and timestamptz with
-- text/varchar/bpchar.
-- ====================================================================
-- Pin the time zone and date style so results are stable across machines.
SET TimeZone = 'UTC';
SET DateStyle = 'ISO, YMD';

-- text -> date.  The unknown-type literal is coerced to text before format
-- cast lookup.
SELECT CAST('2026-06-28' AS date FORMAT 'YYYY-MM-DD');
-- varchar source -> date
SELECT CAST('2026-06-28'::varchar AS date FORMAT 'YYYY-MM-DD');
-- bpchar source -> date: a CHAR(10) holds the value exactly, and a wider
-- CHAR(20) pads with blanks that the format cast trims before parsing.
SELECT CAST('2026-06-28'::char(10) AS date FORMAT 'YYYY-MM-DD');
SELECT CAST('2026-06-28'::char(20) AS date FORMAT 'YYYY-MM-DD');

-- The built-in format cast functions are STRICT: a NULL source or a NULL FORMAT
-- expression yields NULL without invoking the format cast.
SELECT CAST(NULL::text AS date FORMAT 'YYYY-MM-DD') IS NULL AS null_src;
SELECT CAST('2026-06-28' AS date FORMAT NULL::text) IS NULL AS null_fmt;
SELECT CAST(NULL::date AS text FORMAT 'YYYY-MM-DD') IS NULL AS null_src_out;

-- date -> text / varchar
SELECT CAST(date '2026-06-28' AS text FORMAT 'YYYY-MM-DD');
SELECT CAST(date '2026-06-28' AS varchar FORMAT 'YYYY-MM-DD');
-- date -> varchar with a typmod: the declared length is enforced by the
-- ordinary coercion layered above the format cast (here it truncates to 7).
SELECT CAST(date '2026-06-28' AS varchar(7) FORMAT 'YYYY-MM-DD');
-- date -> bpchar: AS char(12) pads to the declared length; bare AS char is
-- char(1), so the ordinary coercion truncates to one character.  The wrapper
-- returns a text varlena that is a valid bpchar value.
SELECT CAST(date '2026-06-28' AS char(12) FORMAT 'YYYY-MM-DD');
SELECT CAST(date '2026-06-28' AS char FORMAT 'YYYY-MM-DD');

-- Collation: a formatted cast is collated like the function call it executes.
-- A collatable result type (text) must get a real result collation, not
-- InvalidOid; "collation for" reports "default" (it would be NULL if the
-- result collation were unset), and an explicit COLLATE on the result works.
SELECT pg_collation_for(CAST(date '2026-06-28' AS text FORMAT 'YYYY-MM-DD'));
SELECT pg_collation_for(CAST(date '2026-06-28' AS text FORMAT 'YYYY-MM-DD') COLLATE "C");
-- An explicit COLLATE on the FORMAT argument flows in as the input collation.
SELECT pg_collation_for(CAST(date '2026-06-28' AS text
                             FORMAT 'YYYY-MM-DD'::text COLLATE "C"));

-- timestamp without time zone, both directions
SELECT CAST('2026-06-28 13:45:30' AS timestamp FORMAT 'YYYY-MM-DD HH24:MI:SS');
SELECT CAST(timestamp '2026-06-28 13:45:30' AS text FORMAT 'YYYY-MM-DD HH24:MI:SS');
-- a date-only template is widened to timestamp (midnight)
SELECT CAST('2026-06-28' AS timestamp FORMAT 'YYYY-MM-DD');
-- The built-ins reuse PostgreSQL's existing formatting parser: a template with
-- no time-zone field consumes only the fields it names, so a trailing time zone
-- in the input is simply ignored (existing to_timestamp() behavior, unchanged).
SELECT CAST('2026-06-28 13:45:30+05' AS timestamp FORMAT 'YYYY-MM-DD HH24:MI:SS');
-- But a template that explicitly contains a time-zone field is rejected for a
-- timestamp-without-time-zone target (we must not fold a zone into a zoneless
-- result via the session TimeZone).
SELECT CAST('2026-06-28 13:45:30+05' AS timestamp FORMAT 'YYYY-MM-DD HH24:MI:SS TZH');
-- timestamp WITHOUT time zone must not depend on the session TimeZone: the same
-- (unzoned) input/template yields the same wall-clock value under any zone.
SET TimeZone = 'America/Los_Angeles';
SELECT CAST('2026-06-28 13:45:30' AS timestamp FORMAT 'YYYY-MM-DD HH24:MI:SS');
SET TimeZone = 'UTC';
SELECT CAST('2026-06-28 13:45:30' AS timestamp FORMAT 'YYYY-MM-DD HH24:MI:SS');

-- timestamp with time zone, both directions (TimeZone is UTC, set above)
SELECT CAST('2026-06-28 13:45:30 +00' AS timestamptz FORMAT 'YYYY-MM-DD HH24:MI:SS TZH');
SELECT CAST(timestamptz '2026-06-28 13:45:30+00' AS text FORMAT 'YYYY-MM-DD HH24:MI:SS TZH');

-- A sampling of datetime template tokens, exercised through the existing
-- to_char/to_timestamp engine.  TZM and SSSSS are supported here.
SELECT CAST(timestamptz '2026-06-28 13:45:30+00' AS text
            FORMAT 'YYYY-MM-DD"T"HH24:MI:SS TZH:TZM');
SELECT CAST(timestamp '2026-06-28 13:45:30' AS text FORMAT 'SSSSS');
-- Fractional seconds (FFn) are accepted by to_char in the output direction.
SELECT CAST(timestamp '2026-06-28 13:45:30.123456' AS text
            FORMAT 'HH24:MI:SS.FF6');

-- Empty FORMAT template behavior follows the underlying formatting functions.
-- In the parse direction the underlying to_date() engine parses no fields and
-- returns the all-defaults date; in the format direction the wrappers mirror
-- to_char(value, ''), which is NULL.
SELECT CAST('2026-06-28' AS date FORMAT '');
SELECT CAST(date '2026-06-28' AS text FORMAT '') IS NULL AS empty_fmt_out;

-- The FORMAT clause remains a PostgreSQL extension: it may be any expression
-- coercible to text, not just a string literal.
SELECT CAST('2026-' || '06-28' AS date FORMAT 'YYYY-' || 'MM-DD');

-- A FORMAT clause never falls back to an ordinary cast: an integer -> date
-- pair has no format cast (and no ordinary cast), so it errors.
SELECT CAST(5 AS date FORMAT 'YYYY');

-- A built-in format cast occupies its (source, target) pair, so a user
-- CREATE FORMAT CAST for the same pair fails with the usual duplicate error.
CREATE FUNCTION my_text_to_date(text, text) RETURNS date
	LANGUAGE sql IMMUTABLE RETURN to_date($1, $2);
CREATE FORMAT CAST (text AS date)
	WITH FUNCTION my_text_to_date(text, text);		-- fails: already exists
DROP FUNCTION my_text_to_date(text, text);

-- Built-in format cast rows are system objects (their OIDs are in the pinned
-- range), so they cannot be dropped.
DROP FORMAT CAST (text AS date);				-- fails: pinned system object

-- A view over a built-in formatted cast deparses back to CAST(... FORMAT ...).
CREATE VIEW builtin_format_cast_view AS
	SELECT CAST('2026-06-28' AS date FORMAT 'YYYY-MM-DD') AS d;
SELECT pg_get_viewdef('builtin_format_cast_view'::regclass, true);
SELECT * FROM builtin_format_cast_view;
DROP VIEW builtin_format_cast_view;

-- Make sure query jumbling handles CoerceViaFormatCast.
SET compute_query_id = on;
SELECT CAST('2026-06-28' AS date FORMAT 'YYYY-MM-DD');
RESET compute_query_id;

RESET TimeZone;
RESET DateStyle;
