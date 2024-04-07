# Copyright (c) 2022-2024, PostgreSQL Global Development Group

binaryheap_bench_sources = files(
  'binaryheap_bench.c',
  'xx_binaryheap.c',
  'old_binaryheap.c',
)

if host_system == 'windows'
  binaryheap_bench_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'binaryheap_bench',
    '--FILEDESC', 'binaryheap_bench',])
endif

binaryheap_bench = shared_module('binaryheap_bench',
  binaryheap_bench_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += binaryheap_bench

install_data(
  'binaryheap_bench--1.0',
  'binaryheap_bench.control',
  kwargs: contrib_data_args,
)
