--
-- CREATE_FUNCTION_1
--

-- directory path and DLSUFFIX are passed to us in environment variables
\getenv LIBDIR LIBDIR
\getenv DLSUFFIX DLSUFFIX

\set regresslib :LIBDIR '/regress' :DLSUFFIX

-- Create C functions needed by create_type.sql

CREATE FUNCTION widget_in(cstring)
   RETURNS widget
   AS :'regresslib'
   LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION widget_out(widget)
   RETURNS cstring
   AS :'regresslib'
   LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION int44in(cstring)
   RETURNS city_budget
   AS :'regresslib'
   LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION int44out(city_budget)
   RETURNS cstring
   AS :'regresslib'
   LANGUAGE C STRICT IMMUTABLE;
