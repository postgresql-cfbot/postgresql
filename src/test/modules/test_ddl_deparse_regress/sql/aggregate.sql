CREATE AGGREGATE newavg(int4) (
   sfunc = int4_avg_accum, stype = _int8,
   finalfunc = int8_avg,
   initcond1 = '{0,0}'
);

DROP AGGREGATE newavg(int4);
