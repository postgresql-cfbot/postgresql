# Copyright (c) 2022-2026, PostgreSQL Global Development Group

libpq_protocol_cursor_sources = files(
  'libpq_protocol_cursor.c',
)

if host_system == 'windows'
  libpq_protocol_cursor_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'libpq_protocol_cursor',
    '--FILEDESC', 'libpq_protocol_cursor - test program for extended query protocol cursors',])
endif

libpq_protocol_cursor = executable('libpq_protocol_cursor',
  libpq_protocol_cursor_sources,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args + {
    'install': false,
  },
)
testprep_targets += libpq_protocol_cursor

tests += {
  'name': 'libpq_protocol_cursor',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_libpq_protocol_cursor.pl',
    ],
    'deps': [libpq_protocol_cursor],
  },
}
