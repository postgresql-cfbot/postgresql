# Copyright (c) 2022-2023, PostgreSQL Global Development Group

test_memtrack_sources = files(
  'test_memtrack.c',
  'worker_pool.c',
)

if host_system == 'windows'
  test_memtrack_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_memtrack',
    '--FILEDESC', 'test_memtrack - test memory tracking',])
endif

test_memtrack = shared_module('test_memtrack',
  test_memtrack_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_memtrack

test_install_data += files(
  'test_memtrack.control',
  'test_memtrack--1.0.sql',
)

tests += {
  'name': 'test_memtrack',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_memtrack',
    ],
    'regress_args': [
      '--temp-config', files('test_memtrack.conf'),
    ],

  },
}
