# Copyright (c) 2023, PostgreSQL Global Development Group

test_wal_read_from_buffers_sources = files(
  'test_wal_read_from_buffers.c',
)

if host_system == 'windows'
  test_wal_read_from_buffers_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_wal_read_from_buffers',
    '--FILEDESC', 'test_wal_read_from_buffers - test module to read WAL from WAL buffers',])
endif

test_wal_read_from_buffers = shared_module('test_wal_read_from_buffers',
  test_wal_read_from_buffers_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_wal_read_from_buffers

test_install_data += files(
  'test_wal_read_from_buffers.control',
  'test_wal_read_from_buffers--1.0.sql',
)

tests += {
  'name': 'test_wal_read_from_buffers',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_basic.pl',
    ],
  },
}
