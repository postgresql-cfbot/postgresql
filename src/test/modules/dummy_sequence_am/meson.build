# Copyright (c) 2022-2023, PostgreSQL Global Development Group

dummy_sequence_am_sources = files(
  'dummy_sequence_am.c',
)

if host_system == 'windows'
  dummy_sequence_am_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'dummy_sequence_am',
    '--FILEDESC', 'dummy_sequence_am - sequence access method template',])
endif

dummy_sequence_am = shared_module('dummy_sequence_am',
  dummy_sequence_am_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += dummy_sequence_am

test_install_data += files(
  'dummy_sequence_am.control',
  'dummy_sequence_am--1.0.sql',
)

tests += {
  'name': 'dummy_sequence_am',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'dummy_sequence',
    ],
  },
}
