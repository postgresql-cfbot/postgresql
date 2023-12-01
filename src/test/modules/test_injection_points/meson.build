# Copyright (c) 2022-2023, PostgreSQL Global Development Group

if not get_option('injection_points')
  subdir_done()
endif

test_injection_points_sources = files(
  'test_injection_points.c',
)

if host_system == 'windows'
  test_injection_points_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_injection_points',
    '--FILEDESC', 'test_injection_points - test injection points',])
endif

test_injection_points = shared_module('test_injection_points',
  test_injection_points_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_injection_points

test_install_data += files(
  'test_injection_points.control',
  'test_injection_points--1.0.sql',
)

tests += {
  'name': 'test_injection_points',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_injection_points',
    ],
  },
  'tap': {
    'tests': [
      't/001_snapshot_status.pl',
      't/002_invalid_checkpoint_after_promote.pl',
    ],
  }
}
