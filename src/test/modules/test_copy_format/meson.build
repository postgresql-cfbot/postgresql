# Copyright (c) 2024, PostgreSQL Global Development Group

test_copy_format_sources = files(
  'test_copy_format.c',
)

if host_system == 'windows'
  test_copy_format_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_copy_format',
    '--FILEDESC', 'test_copy_format - test custom COPY FORMAT',])
endif

test_copy_format = shared_module('test_copy_format',
  test_copy_format_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_copy_format

test_install_data += files(
  'test_copy_format.control',
  'test_copy_format--1.0.sql',
)

tests += {
  'name': 'test_copy_format',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_copy_format',
    ],
  },
}
