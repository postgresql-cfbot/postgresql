# FIXME: prevent install during main install, but not during test :/

test_radixtree_sources = files(
  'test_radixtree.c',
)

if host_system == 'windows'
  test_radixtree_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_radixtree',
    '--FILEDESC', 'test_radixtree - test code for src/backend/lib/radixtree.c',])
endif

test_radixtree = shared_module('test_radixtree',
  test_radixtree_sources,
  kwargs: pg_mod_args,
)
testprep_targets += test_radixtree

install_data(
  'test_radixtree.control',
  'test_radixtree--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'test_radixtree',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_radixtree',
    ],
  },
}
