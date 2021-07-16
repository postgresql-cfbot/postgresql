-- This file has a bunch of kludges needed for upgrading testing across major versions

SELECT
	ver >= 804 AND ver <= 1100 AS fromv84v11,
	ver >= 905 AND ver <= 1300 AS fromv95v13,
	ver >= 906 AND ver <= 1300 AS fromv96v13,
	ver <= 80400 AS fromv84,
	ver <= 90500 AS fromv95,
	ver <= 90600 AS fromv96,
	ver <= 100000 AS fromv10,
	ver <= 110000 AS fromv11,
	ver <= 120000 AS fromv12,
	ver <= 130000 AS fromv13
	FROM (SELECT current_setting('server_version_num')::int/100 AS ver) AS v;
\gset

\if :fromv84
DROP FUNCTION public.myfunc(integer);
\endif

-- last in 9.6 -- commit 5ded4bd21
DROP FUNCTION IF EXISTS public.oldstyle_length(integer, text);
DROP FUNCTION IF EXISTS public.putenv(text);

\if :fromv13
-- last in v13 commit 76f412ab3
-- public.!=- This one is only needed for v11+ ??
-- Note, until v10, operators could only be dropped one at a time
DROP OPERATOR IF EXISTS public.#@# (pg_catalog.int8, NONE);
DROP OPERATOR IF EXISTS public.#%# (pg_catalog.int8, NONE);
DROP OPERATOR IF EXISTS public.!=- (pg_catalog.int8, NONE);
DROP OPERATOR IF EXISTS public.#@%# (pg_catalog.int8, NONE);
\endif

\if :fromv10
-- commit 068503c76511cdb0080bab689662a20e86b9c845
DROP TRANSFORM FOR integer LANGUAGE sql CASCADE;

-- commit db3af9feb19f39827e916145f88fa5eca3130cb2
DROP FUNCTION boxarea(box);
DROP FUNCTION funny_dup17();

-- commit cda6a8d01d391eab45c4b3e0043a1b2b31072f5f
DROP TABLE abstime_tbl;
DROP TABLE reltime_tbl;
DROP TABLE tinterval_tbl;
\endif

\if :fromv96v13
-- Various things removed for v14
DROP AGGREGATE first_el_agg_any(anyelement);
\endif

\if :fromv95v13
-- commit 9e38c2bb5 and 97f73a978
-- DROP AGGREGATE array_larger_accum(anyarray);
DROP AGGREGATE array_cat_accum(anyarray);

-- commit 76f412ab3
-- DROP OPERATOR @#@(bigint,NONE);
DROP OPERATOR @#@(NONE,bigint);
\endif

-- \if :fromv84v11
\if :fromv11
-- commit 578b22971: OIDS removed in v12
ALTER TABLE public.tenk1 SET WITHOUT OIDS;
ALTER TABLE public.tenk1 SET WITHOUT OIDS;
-- fix_sql="$fix_sql ALTER TABLE public.stud_emp SET WITHOUT OIDS;" # inherited
ALTER TABLE public.emp SET WITHOUT OIDS;
ALTER TABLE public.tt7 SET WITHOUT OIDS;
\endif

-- if [ "$newsrc" != "$oldsrc" ]; then
-- 	# update references to old source tree's regress.so etc
-- 	fix_sql=""
-- 	case $oldpgversion in
-- 		804??)
-- 			fix_sql="UPDATE pg_proc SET probin = replace(probin::text, '$oldsrc', '$newsrc')::bytea WHERE probin LIKE '$oldsrc%';"
-- 			;;
-- 		*)
-- 			fix_sql="UPDATE pg_proc SET probin = replace(probin, '$oldsrc', '$newsrc') WHERE probin LIKE '$oldsrc%';"
-- 			;;
-- 	esac
-- 	psql -X -d regression -c "$fix_sql;" || psql_fix_sql_status=$?
--
--	mv "$temp_root"/dump1.sql "$temp_root"/dump1.sql.orig
-- 	sed "s;$oldsrc;$newsrc;g" "$temp_root"/dump1.sql.orig >"$temp_root"/dump1.sql
-- fi
