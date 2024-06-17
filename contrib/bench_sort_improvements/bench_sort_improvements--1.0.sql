create function bench_oid_sort(integer) returns text AS 'MODULE_PATHNAME', 'bench_oid_sort' LANGUAGE C;

create function bench_int_sort(integer) returns text AS 'MODULE_PATHNAME', 'bench_int_sort' LANGUAGE C;

create function bench_int16_sort(integer) returns text AS 'MODULE_PATHNAME', 'bench_int16_sort' LANGUAGE C;

create function bench_float8_sort(integer) returns text AS 'MODULE_PATHNAME', 'bench_float8_sort' LANGUAGE C;
