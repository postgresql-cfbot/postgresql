# Copyright (c) 2024, PostgreSQL Global Development Group

pg_copy_json_sources = files(
  'pg_copy_json.c',
)

if host_system == 'windows'
  pg_copy_json_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_copy_json',
    '--FILEDESC', 'pg_copy_json - COPY TO JSON format',])
endif

pg_copy_json = shared_module('pg_copy_json',
  pg_copy_json_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_copy_json

install_data(
  'pg_copy_json--1.0.sql',
  'pg_copy_json.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_copy_json',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_copy_json',
    ],
  },
}
