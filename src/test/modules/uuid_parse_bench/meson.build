# Copyright (c) 2022-2026, PostgreSQL Global Development Group

uuid_parse_bench_sources = files(
  'uuid_parse_bench.c',
)

if host_system == 'windows'
  uuid_parse_bench_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'uuid_parse_bench',
    '--FILEDESC', 'uuid_parse_bench - microbenchmark for UUID input parsing',])
endif

uuid_parse_bench = shared_module('uuid_parse_bench',
  uuid_parse_bench_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += uuid_parse_bench

test_install_data += files(
  'uuid_parse_bench.control',
  'uuid_parse_bench--1.0.sql',
)

tests += {
  'name': 'uuid_parse_bench',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'uuid_parse_bench',
    ],
  },
}
