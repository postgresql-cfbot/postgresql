\set ON_ERROR_STOP 0

-- Function that returns the protocol version, as required for the protocol
CREATE OR REPLACE FUNCTION pg_protocol(major int, minor int)
RETURNS int
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT $1 << 16 | $2;
$$;

--
-- first a few error checks about acceptance of various protocol versions
--
-- Can't execute failing tests these via psql -f / pg_regress, as they
-- exit after a connection failure, therefore those are commented out.
--

-- -- too old protocol version
-- SELECT pg_protocol(major => 1, minor => 0) \gset
-- \setenv PGFORCEPROTOCOLVERSION :pg_protocol
-- \c
--
-- -- check too low major, with high minor
-- SELECT pg_protocol(major => 1, minor => 30) \gset
-- \setenv PGFORCEPROTOCOLVERSION :pg_protocol
-- \c
--

-- check 2.0 protocol, the earliest accepted
SELECT pg_protocol(major => 2, minor => 0) \gset
\setenv PGFORCEPROTOCOLVERSION :pg_protocol
\c

-- check 2.10 protocol, an unknown minor
SELECT pg_protocol(major => 2, minor => 10) \gset
\setenv PGFORCEPROTOCOLVERSION :pg_protocol
\c

-- check 3.0 protocol, the default version
SELECT pg_protocol(major => 3, minor => 0) \gset
\setenv PGFORCEPROTOCOLVERSION :pg_protocol
\c

-- -- check 3.10 protocol, an unknown minor, will error out
-- SELECT pg_protocol(major => 3, minor => 10) \gset
-- \setenv PGFORCEPROTOCOLVERSION :pg_protocol
-- \c


--
-- ensure some coverage for v2 protocol, which is otherwise untested
--

SELECT pg_protocol(major => 2, minor => 0) \gset
\setenv PGFORCEPROTOCOLVERSION :pg_protocol
\c


-- check that NOTICEs in the middle of a statement work
CREATE OR REPLACE FUNCTION pg_temp.raise_notice(p_text text)
RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE '%', p_text;
    RETURN p_text;
END$$;

-- check that NOTICE in the middle of a normal statement works
SELECT d, COALESCE(d, pg_temp.raise_notice('isnull'))
FROM (VALUES ('notnull'), (NULL), ('notnullagain')) AS d(d);

-- check that NOTICE in the middle of a COPY works
CREATE TEMPORARY TABLE bleat_on_null(d text CHECK(COALESCE(d, pg_temp.raise_notice('isnull')) IS NOT NULL));
\copy bleat_on_null from stdin
notnull
\N
\N
notnullagain
\.
-- while at it, also test COPY OUT
\copy bleat_on_null TO stdout
COPY BLEAT_ON_NULL TO stdout;

-- no columns
SELECT;

-- anonymous columns
SELECT 1;

-- many columns with NULLs (for NULL bitmap)
SELECT 1 c1, 2 c2, 3 c3, NULL::int c4, 5 c5, 6 c6, 7 c7, 8 c8, NULL::text c9, 10 c10;

-- send two sql queries in one protocol message
SELECT 1\;SELECT 2;

-- a few more rows
SELECT a, b
FROM generate_series(1, 10) a, generate_series(1, 10) b;

-- check error responses
SELECT "nonexistant-column";

DO $$
BEGIN
    RAISE 'I am an error, what is your name?';
END;
$$;


-- minimal largeobject test, also verifies PQfn()
BEGIN;
SELECT lo_creat(1) AS lo_oid \gset
SELECT lo_open(:lo_oid, CAST(x'20000' | x'40000' AS integer)) as fd \gset
SELECT lowrite(:fd, 'just some text');
SELECT loread(:fd, '100');
SELECT lo_lseek(:fd, 0, 2);
COMMIT;
\lo_unlink :lo_oid

--
-- Cleanup
--
DROP FUNCTION pg_protocol(int, int);
