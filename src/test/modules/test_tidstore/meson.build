# FIXME: prevent install during main install, but not during test :/

test_tidstore_sources = files(
  'test_tidstore.c',
)

if host_system == 'windows'
  test_tidstore_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_tidstore',
    '--FILEDESC', 'test_tidstore - test code for src/backend/access/common/tidstore.c',])
endif

test_tidstore = shared_module('test_tidstore',
  test_tidstore_sources,
  link_with: pgport_srv,
  kwargs: pg_mod_args,
)
testprep_targets += test_tidstore

install_data(
  'test_tidstore.control',
  'test_tidstore--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'test_tidstore',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_tidstore',
    ],
  },
}
