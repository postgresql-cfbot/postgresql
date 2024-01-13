/*
 * This test is for collations and character operations when using the
 * builtin provider with the C.UTF-8 locale.
 */

/* skip test if not UTF8 server encoding */
SELECT getdatabaseencoding() <> 'UTF8' AS skip_test \gset
\if :skip_test
\quit
\endif

SET client_encoding TO UTF8;

--
-- Test preinstalled PG_C_UTF8 collation.
--

CREATE TABLE builtin_test (
  t TEXT COLLATE PG_C_UTF8
);
INSERT INTO builtin_test VALUES
  ('abc DEF'),
  ('ábc DÉF'),
  ('ǄxxǄ ǆxxǅ ǅxxǆ'),
  ('ȺȺȺ'),
  ('ⱥⱥⱥ'),
  ('ⱥȺ');

SELECT
    t, lower(t), initcap(t), upper(t),
    length(convert_to(t, 'UTF8')) AS t_bytes,
    length(convert_to(lower(t), 'UTF8')) AS lower_t_bytes,
    length(convert_to(initcap(t), 'UTF8')) AS initcap_t_bytes,
    length(convert_to(upper(t), 'UTF8')) AS upper_t_bytes
  FROM builtin_test;

DROP TABLE builtin_test;

-- character classes

SELECT 'xyz' ~ '[[:alnum:]]' COLLATE PG_C_UTF8;
SELECT 'xyz' !~ '[[:upper:]]' COLLATE PG_C_UTF8;
SELECT '@' !~ '[[:alnum:]]' COLLATE PG_C_UTF8;
SELECT '@' ~ '[[:punct:]]' COLLATE PG_C_UTF8; -- symbols are punctuation in posix
SELECT 'a8a' ~ '[[:digit:]]' COLLATE PG_C_UTF8;
SELECT '൧' !~ '\d' COLLATE PG_C_UTF8; -- only 0-9 considered digits in posix

-- case mapping

SELECT 'xYz' ~* 'XyZ' COLLATE PG_C_UTF8;
SELECT 'xAb' ~* '[W-Y]' COLLATE PG_C_UTF8;
SELECT 'xAb' !~* '[c-d]' COLLATE PG_C_UTF8;
SELECT 'Δ' ~* '[α-λ]' COLLATE PG_C_UTF8;
SELECT 'δ' ~* '[Γ-Λ]' COLLATE PG_C_UTF8; -- same as above with cases reversed
