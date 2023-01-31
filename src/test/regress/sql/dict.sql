--
-- Compression dictionaries tests
--

-- Dictionary types currently can be used only with JSONB
\set ON_ERROR_STOP 0
CREATE TYPE textdict AS DICTIONARY OF TEXT ('abcdef', 'ghijkl');
\set ON_ERROR_STOP 1

-- Simple happy path
CREATE TYPE mydict AS DICTIONARY OF JSONB ('abcdef', 'ghijkl');
SELECT dictentry FROM pg_dict ORDER BY dictentry;

SELECT '{"abcdef":"ghijkl"}' :: mydict;
SELECT '{"abcdef":"ghijkl"}' :: mydict :: jsonb;
SELECT '{"abcdef":"ghijkl"}' :: jsonb :: mydict;
SELECT ('{"abcdef":"ghijkl"}' :: mydict) -> 'abcdef';

DROP TYPE mydict;
SELECT dictentry FROM pg_dict ORDER BY dictentry;
