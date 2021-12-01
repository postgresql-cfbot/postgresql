CREATE EXTENSION test_path;

SELECT * FROM test_platform();

-- prefix is '/'
SELECT * FROM test_canonicalize_path('/');
SELECT * FROM test_canonicalize_path('//');
SELECT * FROM test_canonicalize_path('/./abc/def/');
SELECT * FROM test_canonicalize_path('/./../abc/def');
SELECT * FROM test_canonicalize_path('/./../../abc/def/');
SELECT * FROM test_canonicalize_path('/../abc/def');
SELECT * FROM test_canonicalize_path('/../../abc/def');
SELECT * FROM test_canonicalize_path('/abc/../abc/def');
SELECT * FROM test_canonicalize_path('/abc/./../abc/def//');
SELECT * FROM test_canonicalize_path('/abc/../../abc/def');
SELECT * FROM test_canonicalize_path('/abc/def/../../abc/def');
SELECT * FROM test_canonicalize_path('/abc/def/../../../../abc/def');

-- prefix is '/abc'
SELECT * FROM test_canonicalize_path('/abc/./abc/def');
SELECT * FROM test_canonicalize_path('/abc/abc/../abc/def');
SELECT * FROM test_canonicalize_path('/abc/abc/./../abc/def');
SELECT * FROM test_canonicalize_path('/abc/abc/def/../../abc/def');

-- prefix is '.'
SELECT * FROM test_canonicalize_path('.');
SELECT * FROM test_canonicalize_path('./');
SELECT * FROM test_canonicalize_path('./abc/..');

-- prefix is '..'
SELECT * FROM test_canonicalize_path('..');
SELECT * FROM test_canonicalize_path('../');
SELECT * FROM test_canonicalize_path('../abc/def');
SELECT * FROM test_canonicalize_path('./../abc/def');
SELECT * FROM test_canonicalize_path('./abc/../../abc/def');
SELECT * FROM test_canonicalize_path('./abc/./def/.././ghi/../../../abc/def');

-- prefix is 'abc'
SELECT * FROM test_canonicalize_path('abc');
SELECT * FROM test_canonicalize_path('abc/');
SELECT * FROM test_canonicalize_path('./abc/def');
SELECT * FROM test_canonicalize_path('./abc/../abc/def');
SELECT * FROM test_canonicalize_path('./abc/./../abc/def');
SELECT * FROM test_canonicalize_path('./abc/./def/.././../abc/def');
SELECT * FROM test_canonicalize_path('abc/def');
SELECT * FROM test_canonicalize_path('abc/../abc/def');
SELECT * FROM test_canonicalize_path('abc/./../abc/def');
SELECT * FROM test_canonicalize_path('abc/./def/.././../abc/def');

-- special
SELECT * FROM test_canonicalize_path('/abc/c:abc');
SELECT * FROM test_canonicalize_path('/abc/c:/abc');
SELECT * FROM test_canonicalize_path('/abc/c:\abc');
SELECT * FROM test_canonicalize_path('/abc/c:abc/def');
SELECT * FROM test_canonicalize_path('/abc/c:abc/./def');
SELECT * FROM test_canonicalize_path('/abc/c:abc/./../def');
SELECT * FROM test_canonicalize_path('c:abc');
SELECT * FROM test_canonicalize_path('c:abc/def');
SELECT * FROM test_canonicalize_path('./c:abc/./def');
SELECT * FROM test_canonicalize_path('../c:abc/./../def');
SELECT * FROM test_canonicalize_path('/..a/b..');


-- Windows
-- prefix is 'C:\' or '\\network'
SELECT * FROM test_canonicalize_path('C:');
SELECT * FROM test_canonicalize_path('C:\');
SELECT * FROM test_canonicalize_path('C:\\');
SELECT * FROM test_canonicalize_path('C:\\\');
SELECT * FROM test_canonicalize_path('C:\\.\abc\def');
SELECT * FROM test_canonicalize_path('C:\\.\..\abc\def');
SELECT * FROM test_canonicalize_path('C:\.\..\..\abc\def');
SELECT * FROM test_canonicalize_path('C:\..\abc\def');
SELECT * FROM test_canonicalize_path('C:\..\..\abc\def');
SELECT * FROM test_canonicalize_path('C:\abc\..\abc\def');
SELECT * FROM test_canonicalize_path('C:\abc\.\..\abc\def');
SELECT * FROM test_canonicalize_path('C:\abc\..\..\abc\def');
SELECT * FROM test_canonicalize_path('C:\abc\def\..\..\abc\def');
SELECT * FROM test_canonicalize_path('C:\abc\def\..\..\..\..\abc\def');

-- prefix is '\\network'
SELECT * FROM test_canonicalize_path('\\127.0.0.1');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\\');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\\\');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\\.\abc\def');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\\.\..\abc\def');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\.\..\..\abc\def');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\..\abc\def');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\..\..\abc\def');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\abc\..\abc\def');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\abc\.\..\abc\def');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\abc\..\..\abc\def');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\abc\def\..\..\abc\def');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\abc\def\..\..\..\..\abc\def');

-- prefix is 'C:\abc' or '\\network\abc'
SELECT * FROM test_canonicalize_path('\\127.0.0.1\abc\.\abc\def');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\abc\abc\..\abc\def');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\abc\abc\.\..\abc\def');
SELECT * FROM test_canonicalize_path('\\127.0.0.1\abc\abc\def\..\..\abc\def');