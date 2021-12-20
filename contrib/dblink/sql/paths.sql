-- Initialization that requires path substitution.

-- directory paths and DLSUFFIX are passed to us in environment variables
\getenv ABS_SRCDIR ABS_SRCDIR
\getenv LIBDIR LIBDIR
\getenv DLSUFFIX DLSUFFIX

\set regresslib :LIBDIR '/regress' :DLSUFFIX

CREATE FUNCTION setenv(text, text)
   RETURNS void
   AS :'regresslib', 'regress_setenv'
   LANGUAGE C STRICT;

CREATE FUNCTION wait_pid(int)
   RETURNS void
   AS :'regresslib'
   LANGUAGE C STRICT;

\set path :ABS_SRCDIR '/'
\set fnbody 'SELECT setenv(''PGSERVICEFILE'', ' :'path' ' || $1)'
CREATE FUNCTION set_pgservicefile(text) RETURNS void LANGUAGE SQL
    AS :'fnbody';
