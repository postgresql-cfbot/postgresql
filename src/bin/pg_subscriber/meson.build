# Copyright (c) 2023, PostgreSQL Global Development Group

pg_subscriber_sources = files(
  'pg_subscriber.c'
)

if host_system == 'windows'
  pg_subscriber_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
	'--NAME', 'pg_subscriber',
	'--FILEDESC', 'pg_subscriber - create a new logical replica from a standby server',])
endif

pg_subscriber = executable('pg_subscriber',
  pg_subscriber_sources,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args,
)
bin_targets += pg_subscriber

tests += {
  'name': 'pg_subscriber',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_basic.pl',
    ],
  },
}

subdir('po', if_found: libintl)
