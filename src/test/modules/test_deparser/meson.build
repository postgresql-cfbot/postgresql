# Copyright (c) 2022-2023, PostgreSQL Global Development Group

test_deparser_sources = files(
  'test_deparser.c',
)

if host_system == 'windows'
  test_deparser_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_deparser',
    '--FILEDESC', 'test_deparser - allow delay between parsing and execution',])
endif

test_deparser = shared_module('test_deparser',
  test_deparser_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_deparser

test_install_data += files(
  'test_deparser.control',
  'test_deparser--1.0.sql',
)

# TODO regression tests
