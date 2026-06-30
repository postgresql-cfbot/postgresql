dfor_dir = join_paths(meson.project_source_root(), 'src/backend/lib')

# Object files of vect, bitpack and dfor used by these unit-tests are in the same
# directory as this meson.build. Tests don't use ones from src/backend/lib and
# compile different ones for themselves. In Meson/Ninja build system we unite
# them into a static library.

dfor_sources = files(
  join_paths(dfor_dir, 'vect_u16.c'),
  join_paths(dfor_dir, 'bitpack_u16.c'),
  join_paths(dfor_dir, 'dfor_u16.c'),
)

dfor_test_lib = static_library(
  'dfor_test_lib',
  dfor_sources,
  include_directories: [
    include_directories('../../..'), # top_builddir
    include_directories('../'),      # src/test
    postgres_inc,                    # src/include here
  ],
  c_args: ['-DFRONTEND'],
)

# We also build libtap as a static library

libtap = static_library(
  'tap',
  '../../test/libtap/tap.c',
  include_directories:
    include_directories('../../..'), # top_builddir
)

# Each test is an ELF executable

test_names = [
  'test_vect_u16',
  'test_uniqsortvect_u16',
  'test_bitpack_u16',
  'test_dfor_u16',
]

foreach t : test_names
  exe = executable(
    t,
    t + '.c',
    link_with: [
      dfor_test_lib,
      libtap,
      common_static, # Provides pg_printf and other common utilities
      pgport_static,    # Provides OS-portability functions
    ],
	dependencies: [os_deps, libintl],
    include_directories: [
      include_directories('../../..'), # top_builddir
      include_directories('../'),      # src/test
      postgres_inc,                    # src/include here
    ],
    # Backend code often requires these arguments to identify as backend
    c_args: ['-DFRONTEND'],
  )

  test(t, exe, suite: 'dfor')
endforeach
