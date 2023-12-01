
# Find doxygen command
doxygen_exe = find_program('doxygen', required: false)

# if doxygen is not installed, display a message.
if (not doxygen_exe.found())
    message('Install doxygen if you wish to create Doxygen documentation')
endif

# Do we want graphs? Generating graphs can be very slow.
graphs = get_option('doxygen_graphs')
dot = find_program('dot', required: graphs)
if (graphs)
    message('If doxygen takes too long, try disabling graphs. "meson configure -Ddoxygen_graphs=false"')
endif

# build the doxygen 'C' filter
doxygen_filter_c = custom_target('doxy_filter_c',
                               build_by_default: false,
                               input: 'doxy_filter.l',
                               output: 'doxy_filter.c',
                               command: flex_cmd)
doxygen_filter_exe = executable('doxy_filter',
                              doxygen_filter_c,
                              build_by_default: false)

# Source code directories
doxygen_source_dirs = [
                       join_paths(meson.source_root(), 'src'),
                       join_paths(meson.source_root(), 'contrib'),
                      ]

# Configure the Doxyfile
doxygen_config = configuration_data()
doxygen_config.set('INPUT', ' '.join(doxygen_source_dirs))
doxygen_config.set('OUTPUT_DIRECTORY', meson.current_build_dir())
doxygen_config.set('PROJECT_NUMBER', pg_version)
doxygen_config.set('INCLUDE_PATH', ' '.join(postgres_inc_d))
doxygen_config.set('INPUT_FILTER', doxygen_filter_exe.full_path())
doxygen_config.set('STRIP_FROM_PATH', meson.source_root())
if (graphs)
    doxygen_config.set('GRAPHS', 'YES')
else
    doxygen_config.set('GRAPHS', 'NO')
endif

doxyfile = configure_file(input : 'Doxyfile.in',
                          output : 'Doxyfile',
                          configuration : doxygen_config)

# Generate doxygen html from source tree using the 'C' filter.
doxygen_html = custom_target('html',
             input : doxyfile,
             output: 'html',
             command : [doxygen_exe, doxyfile],
             depends: doxygen_filter_exe,
             build_by_default: false)

# use "ninja doxygen" to build doxygen
alias_target('doxygen', doxygen_html)
