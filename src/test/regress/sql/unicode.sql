SELECT getdatabaseencoding() <> 'UTF8' AS skip_test \gset
\if :skip_test
\quit
\endif

SELECT U&'\0061\0308bc' <> U&'\00E4bc' COLLATE "C" AS sanity_check;

SELECT normalize(U&'\0061\0308\24D1c') = U&'\00E4\24D1c' COLLATE "C" AS test_default;
SELECT normalize(U&'\0061\0308\24D1c', NFC) = U&'\00E4\24D1c' COLLATE "C" AS test_nfc;
SELECT normalize(U&'\00E4bc', NFC) = U&'\00E4bc' COLLATE "C" AS test_nfc_idem;
SELECT normalize(U&'\00E4\24D1c', NFD) = U&'\0061\0308\24D1c' COLLATE "C" AS test_nfd;
SELECT normalize(U&'\0061\0308\24D1c', NFKC) = U&'\00E4bc' COLLATE "C" AS test_nfkc;
SELECT normalize(U&'\00E4\24D1c', NFKD) = U&'\0061\0308bc' COLLATE "C" AS test_nfkd;

SELECT "normalize"('abc', 'def');  -- run-time error

SELECT U&'\00E4\24D1c' IS NORMALIZED AS test_default;
SELECT U&'\00E4\24D1c' IS NFC NORMALIZED AS test_nfc;

SELECT num, val,
    val IS NFC NORMALIZED AS NFC,
    val IS NFD NORMALIZED AS NFD,
    val IS NFKC NORMALIZED AS NFKC,
    val IS NFKD NORMALIZED AS NFKD
FROM
  (VALUES (1, U&'\00E4bc'),
          (2, U&'\0061\0308bc'),
          (3, U&'\00E4\24D1c'),
          (4, U&'\0061\0308\24D1c')) vals (num, val)
ORDER BY num;

SELECT is_normalized('abc', 'def');  -- run-time error

SELECT unicode_unescape('\0441\043B\043E\043D');
SELECT unicode_unescape('d!0061t!+000061', '!');
SELECT unicode_unescape('d\u0061t\U00000061');

-- run-time error
SELECT unicode_unescape('wrong: \db99');
SELECT unicode_unescape('wrong: \db99\0061');
SELECT unicode_unescape('wrong: \+00db99\+000061');
SELECT unicode_unescape('wrong: \+2FFFFF');
SELECT unicode_unescape('wrong: \udb99\u0061');
SELECT unicode_unescape('wrong: \U0000db99\U00000061');
SELECT unicode_unescape('wrong: \U002FFFFF');
