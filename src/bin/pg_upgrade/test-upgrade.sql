-- This file has a bunch of kludges needed for testing upgrades across major versions

SELECT
	ver >= 804 AND ver <= 1100 AS oldpgversion_84_11,
	ver >= 905 AND ver <= 1300 AS oldpgversion_95_13,
	ver >= 906 AND ver <= 1300 AS oldpgversion_96_13,
	ver >= 906 AND ver <= 1000 AS oldpgversion_96_10,
	ver >= 1000 AS oldpgversion_ge10,
	ver <= 804 AS oldpgversion_le84,
	ver <= 1300 AS oldpgversion_le13
	FROM (SELECT current_setting('server_version_num')::int/100 AS ver) AS v;
\gset

\if :oldpgversion_le84
DROP FUNCTION public.myfunc(integer);
\endif

-- last in 9.6 -- commit 5ded4bd21
DROP FUNCTION IF EXISTS public.oldstyle_length(integer, text);
DROP FUNCTION IF EXISTS public.putenv(text);

\if :oldpgversion_le13
-- last in v13 commit 76f412ab3
-- public.!=- This one is only needed for v11+ ??
-- Note, until v10, operators could only be dropped one at a time
DROP OPERATOR IF EXISTS public.#@# (pg_catalog.int8, NONE);
DROP OPERATOR IF EXISTS public.#%# (pg_catalog.int8, NONE);
DROP OPERATOR IF EXISTS public.!=- (pg_catalog.int8, NONE);
DROP OPERATOR IF EXISTS public.#@%# (pg_catalog.int8, NONE);
\endif

\if :oldpgversion_ge10
-- commit 068503c76511cdb0080bab689662a20e86b9c845
DROP TRANSFORM FOR integer LANGUAGE sql CASCADE;
\endif

\if :oldpgversion_96_10
-- commit db3af9feb19f39827e916145f88fa5eca3130cb2
DROP FUNCTION boxarea(box);
DROP FUNCTION funny_dup17();

-- commit cda6a8d01d391eab45c4b3e0043a1b2b31072f5f
DROP TABLE abstime_tbl;
DROP TABLE reltime_tbl;
DROP TABLE tinterval_tbl;
\endif

\if :oldpgversion_96_13
-- Various things removed for v14
DROP AGGREGATE first_el_agg_any(anyelement);
\endif

\if :oldpgversion_95_13
-- commit 9e38c2bb5 and 97f73a978
-- DROP AGGREGATE array_larger_accum(anyarray);
DROP AGGREGATE array_cat_accum(anyarray);

-- commit 76f412ab3
-- DROP OPERATOR @#@(bigint,NONE);
DROP OPERATOR @#@(NONE,bigint);
\endif

\if :oldpgversion_84_11
-- commit 578b22971: OIDS removed in v12
ALTER TABLE public.tenk1 SET WITHOUT OIDS;
ALTER TABLE public.tenk1 SET WITHOUT OIDS;
-- fix_sql="$fix_sql ALTER TABLE public.stud_emp SET WITHOUT OIDS;" # inherited
ALTER TABLE public.emp SET WITHOUT OIDS;
ALTER TABLE public.tt7 SET WITHOUT OIDS;
\endif
