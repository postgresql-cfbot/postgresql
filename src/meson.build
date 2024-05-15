# Copyright (c) 2022-2024, PostgreSQL Global Development Group

backend_build_deps = [backend_code]
backend_sources = []
backend_link_with = [pgport_srv, common_srv]

generated_backend_sources = []
post_export_backend_sources = []

# libraries that other subsystems might depend upon first, in their respective
# dependency order. Apart from "interfaces", which we put at the top because
# that speeds up parallel builds significantly since our generated ecpg file
# takes a very long time to build so we want to start that as soon as
# possible. We also move backend/parser to the top for the same reason, but
# also because we want to start building the parser before the ecpg files.
# Otherwise an error in the grammer would confusingly show up as an issue in
# the ecpg file.

subdir('backend/parser')

subdir('interfaces')

subdir('timezone')

subdir('backend')

subdir('bin')

subdir('pl')

subdir('tools/pg_bsd_indent')


### Generate a Makefile.global that's complete enough for PGXS to work.
#
# This is somewhat ugly, but allows extensions to use a single buildsystem
# across all the supported postgres versions. Once all supported PG versions
# support meson, we can remove all of this.
#
# XXX: Should we make this optional?

# pgxs_cdata is built in makefiles/meson.build, but some of the generated
# files are output into src/
subdir('makefiles')

makefile_global = configure_file(
  input: 'Makefile.global.in',
  output: 'Makefile.global',
  configuration: pgxs_cdata,
  install: true,
  install_dir: dir_pgxs / 'src',
)
configure_files += makefile_global

makefile_port = configure_file(
  input: 'makefiles' / 'Makefile.@0@'.format(portname),
  output: 'Makefile.port',
  copy: true,
  install_dir: dir_pgxs / 'src')
configure_files += makefile_port

install_data(
  'Makefile.shlib', 'nls-global.mk',
  install_dir: dir_pgxs / 'src')

install_data(
  'makefiles/pgxs.mk',
  install_dir: dir_pgxs / 'src' / 'makefiles')
