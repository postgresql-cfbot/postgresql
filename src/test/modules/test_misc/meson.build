# Copyright (c) 2022-2024, PostgreSQL Global Development Group

test_misc_sources = files(
    'test_misc.c',
)

if host_system == 'windows'
  test_misc_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_misc',
    '--FILEDESC', 'test_misc - ',])
endif

test_misc = shared_module('test_misc',
  test_misc_sources,
  kwargs: pg_test_mod_args,
)

test_install_libs += test_misc

tests += {
  'name': 'test_misc',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_constraint_validation.pl',
      't/002_tablespace.pl',
      't/003_check_guc.pl',
      't/004_io_direct.pl',
    ],
  },
}
