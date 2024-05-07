# Copyright (c) 2024, PostgreSQL Global Development Group

snowflake_sources = files(
  'snowflake.c',
)

if host_system == 'windows'
  snowflake_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'snowflake',
    '--FILEDESC', 'snowflake - sequence access method',])
endif

snowflake = shared_module('snowflake',
  snowflake_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += bloom

install_data(
  'snowflake.control',
  'snowflake--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'snowflake',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'snowflake',
    ],
  },
}
