# Copyright (c) 2022-2023, PostgreSQL Global Development Group

# FIXME: prevent install during main install, but not during test :/

test_dsa_sources = files(
  'test_dsa.c',
)

if host_system == 'windows'
  test_dsa_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_dsa',
    '--FILEDESC', 'test_dsa - test code for dsa.c',])
endif

test_dsa = shared_module('test_dsa',
  test_dsa_sources,
  kwargs: pg_mod_args,
)
testprep_targets += test_dsa

install_data(
  'test_dsa.control',
  'test_dsa--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'test_dsa',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_dsa',
    ],
  },
}
