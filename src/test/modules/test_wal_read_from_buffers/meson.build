# Copyright (c) 2022-2023, PostgreSQL Global Development Group

# FIXME: prevent install during main install, but not during test :/

test_wal_read_from_buffers_sources = files(
  'test_wal_read_from_buffers.c',
)

if host_system == 'windows'
  test_wal_read_from_buffers_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_wal_read_from_buffers',
    '--FILEDESC', 'test_wal_read_from_buffers - test module to verify that WAL can be read from WAL buffers',])
endif

test_wal_read_from_buffers = shared_module('test_wal_read_from_buffers',
  test_wal_read_from_buffers_sources,
  kwargs: pg_mod_args,
)
testprep_targets += test_wal_read_from_buffers

install_data(
  'test_wal_read_from_buffers.control',
  'test_wal_read_from_buffers--1.0.sql',
  kwargs: contrib_data_args,
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
