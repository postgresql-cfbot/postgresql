-- Test non-ASCII error messages
--
-- This test case would fail if the database encoding is EUC_CN, EUC_JP,
-- EUC_KR, or EUC_TW, for lack of any equivalent to U+00A0 (no-break space) in
-- those encodings.  However, testing with plain ASCII data would be rather
-- useless, so we must live with that.
SELECT pg_database_encoding() IN ('EUC_CN', 'EUC_JP', 'EUC_KR', 'EUC_TW')
  AS skip_test \gset
\if :skip_test
\quit
\endif

SET client_encoding TO UTF8;

create or replace function error_with_nbsp() returns void language plperl as $$
  elog(ERROR, "this message contains a no-break space");
$$;

select error_with_nbsp();
