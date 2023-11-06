####################################################################
# Create an archive library containing all the backend object files.
####################################################################
test_lib = static_library('test_lib',
   objects: backend_objs,
   )


###################################################
# Individual unit tests in the 'iostack' test suite.
#  TODO: refactor so each is a single line?
###################################################

# Test the basic File layer
storage_vfdtest = executable('storage_vfdtest',
  files('vfdTest.c',  'framework/fileFramework.c', ),
  include_directories: [include_directories('.'), postgres_inc],
  link_args: backend_link_args,
  link_with: [test_lib, backend_link_with],
  link_depends: backend_link_depends,
)
test('storage/vfd', storage_vfdtest, suite: 'iostack')
