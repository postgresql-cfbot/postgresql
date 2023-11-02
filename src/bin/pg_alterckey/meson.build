pg_alterckey_sources = files(
  'pg_alterckey.c',
)

if host_system == 'windows'
  pg_alterckey_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_alterckey',
    '--FILEDESC', 'pg_alterckey - alter the cluster key',])
endif

pg_alterckey = executable('pg_alterckey',
  pg_alterckey_sources,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args,
)
bin_targets += pg_alterckey
