# Copyright (c) 2024, PostgreSQL Global Development Group

read_wal_from_buffers_sources = files(
  'read_wal_from_buffers.c',
)

if host_system == 'windows'
  read_wal_from_buffers_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'read_wal_from_buffers',
    '--FILEDESC', 'read_wal_from_buffers - test module to read WAL from WAL buffers',])
endif

read_wal_from_buffers = shared_module('read_wal_from_buffers',
  read_wal_from_buffers_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += read_wal_from_buffers

test_install_data += files(
  'read_wal_from_buffers.control',
  'read_wal_from_buffers--1.0.sql',
)

tests += {
  'name': 'read_wal_from_buffers',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_basic.pl',
    ],
  },
}
