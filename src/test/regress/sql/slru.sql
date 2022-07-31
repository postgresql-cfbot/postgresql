--
-- Unit tests for slru.c
--

-- directory paths and dlsuffix are passed to us in environment variables
\getenv libdir PG_LIBDIR
\getenv dlsuffix PG_DLSUFFIX

\set regresslib :libdir '/regress' :dlsuffix

CREATE FUNCTION test_slru() RETURNS VOID
    AS :'regresslib', 'test_slru' LANGUAGE C STRICT;

-- The test is executed twice to make sure the state is cleaned up properly.
SELECT test_slru();
SELECT test_slru();