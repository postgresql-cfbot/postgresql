bench_radix_tree_sources = files(
  'bench_radix_tree.c',
)

if host_system == 'windows'
  bench_radix_tree_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'bench_radix_tree',
    '--FILEDESC', 'bench_radix_tree - performance test code for radix tree',])
endif

bench_radix_tree = shared_module('bench_radix_tree',
  bench_radix_tree_sources,
  link_with: pgport_srv,
  kwargs: pg_mod_args,
)
testprep_targets += bench_radix_tree

install_data(
  'bench_radix_tree.control',
  'bench_radix_tree--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'bench_radix_tree',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'bench_radix_tree',
    ],
  },
}
