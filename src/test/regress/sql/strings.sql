--
-- STRINGS
-- Test various data entry syntaxes.
--

-- SQL string continuation syntax
-- E021-03 character string literals
SELECT 'first line'
' - next line'
	' - third line'
	AS "Three lines to one";

-- illegal string continuation syntax
SELECT 'first line'
' - next line' /* this comment is not allowed here */
' - third line'
	AS "Illegal comment within continuation";

-- Unicode escapes
SET standard_conforming_strings TO on;

SELECT U&'d\0061t\+000061' AS U&"d\0061t\+000061";
SELECT U&'d!0061t\+000061' UESCAPE '!' AS U&"d*0061t\+000061" UESCAPE '*';
SELECT U&'a\\b' AS "a\b";

SELECT U&' \' UESCAPE '!' AS "tricky";
SELECT 'tricky' AS U&"\" UESCAPE '!';

SELECT U&'wrong: \061';
SELECT U&'wrong: \+0061';
SELECT U&'wrong: +0061' UESCAPE +;
SELECT U&'wrong: +0061' UESCAPE '+';

SELECT U&'wrong: \db99';
SELECT U&'wrong: \db99xy';
SELECT U&'wrong: \db99\\';
SELECT U&'wrong: \db99\0061';
SELECT U&'wrong: \+00db99\+000061';
SELECT U&'wrong: \+2FFFFF';

-- while we're here, check the same cases in E-style literals
SELECT E'd\u0061t\U00000061' AS "data";
SELECT E'a\\b' AS "a\b";
SELECT E'wrong: \u061';
SELECT E'wrong: \U0061';
SELECT E'wrong: \udb99';
SELECT E'wrong: \udb99xy';
SELECT E'wrong: \udb99\\';
SELECT E'wrong: \udb99\u0061';
SELECT E'wrong: \U0000db99\U00000061';
SELECT E'wrong: \U002FFFFF';

SET standard_conforming_strings TO off;

SELECT U&'d\0061t\+000061' AS U&"d\0061t\+000061";
SELECT U&'d!0061t\+000061' UESCAPE '!' AS U&"d*0061t\+000061" UESCAPE '*';

SELECT U&' \' UESCAPE '!' AS "tricky";
SELECT 'tricky' AS U&"\" UESCAPE '!';

SELECT U&'wrong: \061';
SELECT U&'wrong: \+0061';
SELECT U&'wrong: +0061' UESCAPE '+';

RESET standard_conforming_strings;

-- bytea
SET bytea_output TO hex;
SELECT E'\\xDeAdBeEf'::bytea;
SELECT E'\\x De Ad Be Ef '::bytea;
SELECT E'\\xDeAdBeE'::bytea;
SELECT E'\\xDeAdBeEx'::bytea;
SELECT E'\\xDe00BeEf'::bytea;
SELECT E'DeAdBeEf'::bytea;
SELECT E'De\\000dBeEf'::bytea;
SELECT E'De\123dBeEf'::bytea;
SELECT E'De\\123dBeEf'::bytea;
SELECT E'De\\678dBeEf'::bytea;

SET bytea_output TO escape;
SELECT E'\\xDeAdBeEf'::bytea;
SELECT E'\\x De Ad Be Ef '::bytea;
SELECT E'\\xDe00BeEf'::bytea;
SELECT E'DeAdBeEf'::bytea;
SELECT E'De\\000dBeEf'::bytea;
SELECT E'De\\123dBeEf'::bytea;

--
-- test conversions between various string types
-- E021-10 implicit casting among the character data types
--

SELECT CAST(f1 AS text) AS "text(char)" FROM CHAR_TBL;

SELECT CAST(f1 AS text) AS "text(varchar)" FROM VARCHAR_TBL;

SELECT CAST(name 'namefield' AS text) AS "text(name)";

-- since this is an explicit cast, it should truncate w/o error:
SELECT CAST(f1 AS char(10)) AS "char(text)" FROM TEXT_TBL;
-- note: implicit-cast case is tested in char.sql

SELECT CAST(f1 AS char(20)) AS "char(text)" FROM TEXT_TBL;

SELECT CAST(f1 AS char(10)) AS "char(varchar)" FROM VARCHAR_TBL;

SELECT CAST(name 'namefield' AS char(10)) AS "char(name)";

SELECT CAST(f1 AS varchar) AS "varchar(text)" FROM TEXT_TBL;

SELECT CAST(f1 AS varchar) AS "varchar(char)" FROM CHAR_TBL;

SELECT CAST(name 'namefield' AS varchar) AS "varchar(name)";

--
-- test SQL string functions
-- E### and T### are feature reference numbers from SQL99
--

-- E021-09 trim function
SELECT TRIM(BOTH FROM '  bunch o blanks  ') = 'bunch o blanks' AS "bunch o blanks";

SELECT TRIM(LEADING FROM '  bunch o blanks  ') = 'bunch o blanks  ' AS "bunch o blanks  ";

SELECT TRIM(TRAILING FROM '  bunch o blanks  ') = '  bunch o blanks' AS "  bunch o blanks";

SELECT TRIM(BOTH 'x' FROM 'xxxxxsome Xsxxxxx') = 'some Xs' AS "some Xs";

-- E021-06 substring expression
SELECT SUBSTRING('1234567890' FROM 3) = '34567890' AS "34567890";

SELECT SUBSTRING('1234567890' FROM 4 FOR 3) = '456' AS "456";

-- test overflow cases
SELECT SUBSTRING('string' FROM 2 FOR 2147483646) AS "tring";
SELECT SUBSTRING('string' FROM -10 FOR 2147483646) AS "string";
SELECT SUBSTRING('string' FROM -10 FOR -2147483646) AS "error";

-- T581 regular expression substring (with SQL's bizarre regexp syntax)
SELECT SUBSTRING('abcdefg' SIMILAR 'a#"(b_d)#"%' ESCAPE '#') AS "bcd";
-- obsolete SQL99 syntax
SELECT SUBSTRING('abcdefg' FROM 'a#"(b_d)#"%' FOR '#') AS "bcd";

-- No match should return NULL
SELECT SUBSTRING('abcdefg' SIMILAR '#"(b_d)#"%' ESCAPE '#') IS NULL AS "True";

-- Null inputs should return NULL
SELECT SUBSTRING('abcdefg' SIMILAR '%' ESCAPE NULL) IS NULL AS "True";
SELECT SUBSTRING(NULL SIMILAR '%' ESCAPE '#') IS NULL AS "True";
SELECT SUBSTRING('abcdefg' SIMILAR NULL ESCAPE '#') IS NULL AS "True";

-- The first and last parts should act non-greedy
SELECT SUBSTRING('abcdefg' SIMILAR 'a#"%#"g' ESCAPE '#') AS "bcdef";
SELECT SUBSTRING('abcdefg' SIMILAR 'a*#"%#"g*' ESCAPE '#') AS "abcdefg";

-- Vertical bar in any part affects only that part
SELECT SUBSTRING('abcdefg' SIMILAR 'a|b#"%#"g' ESCAPE '#') AS "bcdef";
SELECT SUBSTRING('abcdefg' SIMILAR 'a#"%#"x|g' ESCAPE '#') AS "bcdef";
SELECT SUBSTRING('abcdefg' SIMILAR 'a#"%|ab#"g' ESCAPE '#') AS "bcdef";

-- Can't have more than two part separators
SELECT SUBSTRING('abcdefg' SIMILAR 'a*#"%#"g*#"x' ESCAPE '#') AS "error";

-- Postgres extension: with 0 or 1 separator, assume parts 1 and 3 are empty
SELECT SUBSTRING('abcdefg' SIMILAR 'a#"%g' ESCAPE '#') AS "bcdefg";
SELECT SUBSTRING('abcdefg' SIMILAR 'a%g' ESCAPE '#') AS "abcdefg";

-- substring() with just two arguments is not allowed by SQL spec;
-- we accept it, but we interpret the pattern as a POSIX regexp not SQL
SELECT SUBSTRING('abcdefg' FROM 'c.e') AS "cde";

-- With a parenthesized subexpression, return only what matches the subexpr
SELECT SUBSTRING('abcdefg' FROM 'b(.*)f') AS "cde";

-- Check behavior of SIMILAR TO, which uses largely the same regexp variant
SELECT 'abcdefg' SIMILAR TO '_bcd%' AS true;
SELECT 'abcdefg' SIMILAR TO 'bcd%' AS false;
SELECT 'abcdefg' SIMILAR TO '_bcd#%' ESCAPE '#' AS false;
SELECT 'abcd%' SIMILAR TO '_bcd#%' ESCAPE '#' AS true;
-- Postgres uses '\' as the default escape character, which is not per spec
SELECT 'abcdefg' SIMILAR TO '_bcd\%' AS false;
-- and an empty string to mean "no escape", which is also not per spec
SELECT 'abcd\efg' SIMILAR TO '_bcd\%' ESCAPE '' AS true;
-- these behaviors are per spec, though:
SELECT 'abcdefg' SIMILAR TO '_bcd%' ESCAPE NULL AS null;
SELECT 'abcdefg' SIMILAR TO '_bcd#%' ESCAPE '##' AS error;

-- Test back reference in regexp_replace
SELECT regexp_replace('1112223333', E'(\\d{3})(\\d{3})(\\d{4})', E'(\\1) \\2-\\3');
SELECT regexp_replace('AAA   BBB   CCC   ', E'\\s+', ' ', 'g');
SELECT regexp_replace('AAA', '^|$', 'Z', 'g');
SELECT regexp_replace('AAA aaa', 'A+', 'Z', 'gi');
-- invalid regexp option
SELECT regexp_replace('AAA aaa', 'A+', 'Z', 'z');

-- set so we can tell NULL from empty string
\pset null '\\N'

-- return all matches from regexp
SELECT regexp_matches('foobarbequebaz', $re$(bar)(beque)$re$);

-- test case insensitive
SELECT regexp_matches('foObARbEqUEbAz', $re$(bar)(beque)$re$, 'i');

-- global option - more than one match
SELECT regexp_matches('foobarbequebazilbarfbonk', $re$(b[^b]+)(b[^b]+)$re$, 'g');

-- empty capture group (matched empty string)
SELECT regexp_matches('foobarbequebaz', $re$(bar)(.*)(beque)$re$);
-- no match
SELECT regexp_matches('foobarbequebaz', $re$(bar)(.+)(beque)$re$);
-- optional capture group did not match, null entry in array
SELECT regexp_matches('foobarbequebaz', $re$(bar)(.+)?(beque)$re$);

-- no capture groups
SELECT regexp_matches('foobarbequebaz', $re$barbeque$re$);

-- start/end-of-line matches are of zero length
SELECT regexp_matches('foo' || chr(10) || 'bar' || chr(10) || 'bequq' || chr(10) || 'baz', '^', 'mg');
SELECT regexp_matches('foo' || chr(10) || 'bar' || chr(10) || 'bequq' || chr(10) || 'baz', '$', 'mg');
SELECT regexp_matches('1' || chr(10) || '2' || chr(10) || '3' || chr(10) || '4' || chr(10), '^.?', 'mg');
SELECT regexp_matches(chr(10) || '1' || chr(10) || '2' || chr(10) || '3' || chr(10) || '4' || chr(10), '.?$', 'mg');
SELECT regexp_matches(chr(10) || '1' || chr(10) || '2' || chr(10) || '3' || chr(10) || '4', '.?$', 'mg');

-- give me errors
SELECT regexp_matches('foobarbequebaz', $re$(bar)(beque)$re$, 'gz');
SELECT regexp_matches('foobarbequebaz', $re$(barbeque$re$);
SELECT regexp_matches('foobarbequebaz', $re$(bar)(beque){2,1}$re$);

-- split string on regexp
SELECT foo, length(foo) FROM regexp_split_to_table('the quick brown fox jumps over the lazy dog', $re$\s+$re$) AS foo;
SELECT regexp_split_to_array('the quick brown fox jumps over the lazy dog', $re$\s+$re$);

SELECT foo, length(foo) FROM regexp_split_to_table('the quick brown fox jumps over the lazy dog', $re$\s*$re$) AS foo;
SELECT regexp_split_to_array('the quick brown fox jumps over the lazy dog', $re$\s*$re$);
SELECT foo, length(foo) FROM regexp_split_to_table('the quick brown fox jumps over the lazy dog', '') AS foo;
SELECT regexp_split_to_array('the quick brown fox jumps over the lazy dog', '');
-- case insensitive
SELECT foo, length(foo) FROM regexp_split_to_table('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'i') AS foo;
SELECT regexp_split_to_array('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'i');
-- no match of pattern
SELECT foo, length(foo) FROM regexp_split_to_table('the quick brown fox jumps over the lazy dog', 'nomatch') AS foo;
SELECT regexp_split_to_array('the quick brown fox jumps over the lazy dog', 'nomatch');
-- some corner cases
SELECT regexp_split_to_array('123456','1');
SELECT regexp_split_to_array('123456','6');
SELECT regexp_split_to_array('123456','.');
SELECT regexp_split_to_array('123456','');
SELECT regexp_split_to_array('123456','(?:)');
SELECT regexp_split_to_array('1','');
-- errors
SELECT foo, length(foo) FROM regexp_split_to_table('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'zippy') AS foo;
SELECT regexp_split_to_array('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'iz');
-- global option meaningless for regexp_split
SELECT foo, length(foo) FROM regexp_split_to_table('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'g') AS foo;
SELECT regexp_split_to_array('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'g');

-- regexp_like tests
SELECT regexp_like('a'||CHR(10)||'d', 'a.d');
SELECT regexp_like('a'||CHR(10)||'d', 'a.d', 'm');
SELECT regexp_like('a'||CHR(10)||'d', 'a.d', 'n');
SELECT regexp_like('Steven', '^Ste(v|ph)en$');
SELECT regexp_like('foo' || chr(10) || 'bar' || chr(10) || 'bequq' || chr(10) || 'baz', '^bar');
SELECT regexp_like('foo' || chr(10) || 'bar' || chr(10) || 'bequq' || chr(10) || 'baz', 'bar');
SELECT regexp_like('foo' || chr(10) || 'bar' || chr(10) || 'bequq' || chr(10) || 'baz', '^bar', 'm');
SELECT regexp_like('foo' || chr(10) || 'bar' || chr(10) || 'bequq' || chr(10) || 'baz', '^bar', 'n');
SELECT regexp_like('GREEN', '([aeiou])\1');
SELECT regexp_like('GREEN', '([aeiou])\1', 'i');
SELECT regexp_like('ORANGE' || chr(10) || 'GREEN', '([aeiou])\1', 'i');
SELECT regexp_like('ORANGE' || chr(10) || 'GREEN', '^..([aeiou])\1', 'i');
SELECT regexp_like('ORANGE' || chr(10) || 'GREEN', '([aeiou])\1', 'in');
SELECT regexp_like('ORANGE' || chr(10) || 'GREEN', '^..([aeiou])\1', 'in');
SELECT regexp_like('ORANGE' || chr(10) || 'GREEN', '^..([aeiou])\1', 'im');
SELECT REGEXP_LIKE('abc', 'a b c');
SELECT REGEXP_LIKE('abc', 'a b c','x');

--  count all matches for regexp
SELECT regexp_count('123123123123123', '(12)3');
-- count all matches with start position
SELECT regexp_count('123123123123', '123', 0);
SELECT regexp_count('123123123123', '123', 1);
SELECT regexp_count('123123123123', '123', 3);
SELECT regexp_count('123123123123', '123', -3);
-- count all matches in NULL string with a start position
SELECT regexp_count(NULL, '123', 3);
-- count all matches with a start position greater than string length
SELECT regexp_count('123', '123', 10);
-- count all matches from different regexp
SELECT regexp_count('ABC123', '[A-Z]'), regexp_count('A1B2C3', '[A-Z]');
SELECT regexp_count('ABC123', '[A-Z][0-9]'), regexp_count('A1B2C3', '[A-Z][0-9]');
SELECT regexp_count('ABC123', '[A-Z][0-9]{2}'), regexp_count('A1B2C3', '[A-Z][0-9]{2}');
SELECT regexp_count('ABC123', '([A-Z][0-9]){2}'), regexp_count('A1B2C3', '([A-Z][0-9]){2}');
SELECT regexp_count('ABC123A5', '^[A-Z][0-9]'), regexp_count('A1B2C3', '^[A-Z][0-9]');
SELECT regexp_count('ABC123', '[A-Z][0-9]{2}'), regexp_count('A1B2C34', '[A-Z][0-9]{2}');
-- count matches with newline case insensivity
SELECT regexp_count('a'||CHR(10)||'d', 'a.d');
SELECT regexp_count('a'||CHR(10)||'d', 'a.d', 1, 's');
-- count matches with newline case sensivity
SELECT regexp_count('a'||CHR(10)||'d', 'a.d', 1, 'n');
-- count not multiline matches
SELECT regexp_count('a'||CHR(10)||'d', '^d$');
-- count multiline matches
SELECT regexp_count('a'||CHR(10)||'d', '^d$', 1, 'm');
SELECT regexp_count('Hello'||CHR(10)||'world!', '^world!$', 1, 'm'); -- 1
-- Count the number of occurrences of the substring an in the string.
SELECT regexp_count('a man, a plan, a canal: Panama', 'an');
-- Find the number of occurrences of the substring an in the string starting with the fifth character.
SELECT regexp_count('a man, a plan, a canal: Panama', 'an',5);
-- Find the number of occurrences of a substring containing a lower-case character
-- followed by an. In the first example, do not use a modifier. In the second example,
-- use the i modifier to force the regular expression to ignore case.
SELECT regexp_count('a man, a plan, a canal: Panama', '[a-z]an');
SELECT regexp_count('a man, a plan, a canal: Panama', '[a-z]an', 1, 'i');

DROP TABLE IF EXISTS regexp_temp;
CREATE TABLE regexp_temp(fullname varchar(20), email varchar(20));
INSERT INTO regexp_temp (fullname, email) VALUES ('John Doe', 'johndoe@example.com');
INSERT INTO regexp_temp (fullname, email) VALUES ('Jane Doe', 'janedoe');
-- count matches case sensitive
SELECT fullname, regexp_count(fullname, 'e', 1, 'c') FROM regexp_temp;
SELECT fullname, regexp_count(fullname, 'D', 1, 'c') FROM regexp_temp;
SELECT fullname, regexp_count(fullname, 'd', 1, 'c') FROM regexp_temp;
-- count matches case insensitive
SELECT fullname, regexp_count(fullname, 'E', 1, 'i') FROM regexp_temp;
SELECT fullname, regexp_count(fullname, 'do', 1, 'i') FROM regexp_temp;

-- return the start position of the 6th occurence starting at beginning of the string
SELECT regexp_instr('number of your street, zipcode thetown, FR', '[^ ]+', 1, 6);
-- return the start position of the 5th occurence starting after the first word
SELECT regexp_instr('number of your street, zipcode thetown, FR', '[^ ]+', 7, 5, 0);
-- return the ending position of the 5th occurence starting after the first word
SELECT regexp_instr('number of your street, zipcode thetown, FR', '[^ ]+', 7, 5, 0);
-- return the ending position of the 2nd occurence starting after the first word
SELECT regexp_instr('number of your street, zipcode thetown, FR', '[T|Z|S][[:alpha:]]{5}', 7, 2, 1, 'i');
-- return the starting position corresponding to the different capture group
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', 0);
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', 1);
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', 2);
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', 3);
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', 4);
-- return the starting position corresponding to a non existant capture group
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', 5);
-- Same but with the ending position
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, 1, 'i', 0);
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, 1, 'i', 1);
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, 1, 'i', 2);
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, 1, 'i', 3);
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, 1, 'i', 4);
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, 1, 'i', 5);
-- start position of a valid email
SELECT email, regexp_instr(email, '\w+@\w+(\.\w+)+') "valid_email" FROM regexp_temp;
-- ending position of a valid email
SELECT email, regexp_instr(email, '\w+@\w+(\.\w+)+', 1, 1, 1) "valid_email" FROM regexp_temp;
-- start position of first capture group in the email (the dot part)
SELECT email, regexp_instr(email, '\w+@\w+(\.\w+)+', 1, 1, 0, 'i', 1) FROM regexp_temp;
-- ending position of first capture group in the email (the dot part)
SELECT email, regexp_instr(email, '\w+@\w+(\.\w+)+', 1, 1, 1, 'i', 1) FROM regexp_temp;
-- test negative values
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', -1, 1, 1, 'i', 1);
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, -1, 1, 'i', 1);
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, -1, 'i', 1);
SELECT regexp_instr('1234567890', '(123)(4(56)(78))', 1, 1, 1, 'i', -1);

-- Find the first occurrence of a sequence of letters starting with the letter e
-- and ending with the letter y in the phrase "easy come, easy go."
SELECT regexp_instr('easy come, easy go','e\w*y');
-- Find the first sequence of letters starting with the letter e and ending with
-- the letter y in the string "easy come, easy go" starting at the second character
SELECT regexp_instr('easy come, easy go','e\w*y',2);
-- Find the second sequence of letters starting with the letter e and ending with
-- the letter y in the string "easy come, easy go" starting at the first character.
SELECT regexp_instr('easy come, easy go','e\w*y',1,2);
-- Find the position of the first character after the first whitespace in the string "easy come, easy go."
SELECT regexp_instr('easy come, easy go','\s',1,1,1);
-- Find the position of the start of the third word in a string by capturing each
-- word as a subexpression, and returning the third subexpression's start position.
SELECT regexp_instr('one two three','(\w+)\s+(\w+)\s+(\w+)', 1,1,0,'',3);

-- return the substring matching the regexp
SELECT regexp_substr('number of your street, zipcode town, FR', ',[^,]+');
-- return the substring matching the regexp
SELECT regexp_substr('number of your street, zipcode town, FR', ',[^,]+', 24);
-- return the substring matching the regexp at the first occurrence
SELECT regexp_substr('number of your street, zipcode town, FR', ',[^,]+', 1, 1);
-- return the substring matching the regexp at the second occurrence
SELECT regexp_substr('number of your street, zipcode town, FR', ',[^,]+', 1, 2);
-- case sensitivity substring search
SELECT regexp_substr('number of your street, zipcode town, FR', ',\s+[Zf][^,]+', 1, 1);
SELECT regexp_substr('number of your street, zipcode town, FR', ',\s+[Zf][^,]+', 1, 1, 'i');
-- case sensitivity substring search with no capture group
SELECT regexp_substr('number of your street, zipcode town, FR', ',\s+[Zf][^,]+', 1, 1, 'i', 0);
-- case sensitivity substring search with non existing capture group
SELECT regexp_substr('number of your street, zipcode town, FR', ',\s+[Zf][^,]+', 1, 1, 'i', 0);
-- return the substring matching the regexp at different occurrence and capture group
SELECT regexp_substr('1234567890 1234567890', '(123)(4(56)(78))', 1, 1, 'i', 4);
SELECT regexp_substr('1234567890 1234557890', '(123)(4(5[56])(78))', 1, 2, 'i', 3);
SELECT regexp_substr('1234567890 1234567890', '(123)(4(56)(78))', 1, 1, 'i', 0);
-- test negative values
SELECT regexp_substr('1234567890 1234567890', '(123)(4(56)(78))', -1, 1, 'i', 4);
SELECT regexp_substr('1234567890 1234567890', '(123)(4(56)(78))', 1, -1, 'i', 4);
SELECT regexp_substr('1234567890 1234567890', '(123)(4(56)(78))', 1, 1, 'i', -4);
-- Select the first substring of letters that end with "thy."
SELECT regexp_substr('healthy, wealthy, and wise','\w+thy');
-- Select the first substring of letters that ends with "thy" starting at the second character in the string.
SELECT regexp_substr('healthy, wealthy, and wise','\w+thy',2);
-- Return the contents of the third captured subexpression, which captures the third word in the string.
SELECT regexp_substr('one two three', '(\w+)\s+(\w+)\s+(\w+)', 1, 1, '', 3);

DROP TABLE IF EXISTS regexp_temp;

-- Regression tests for extended regexp_replace() function with start position and occurrence
SELECT regexp_replace('512.123.4567', '([[:digit:]]{3})\.([[:digit:]]{3})\.([[:digit:]]{4})', '(\1) \2-\3', 1);
SELECT regexp_replace('512.123.4567 612.123.4567', '([[:digit:]]{3})\.([[:digit:]]{3})\.([[:digit:]]{4})', '(\1) \2-\3', 1, 0);
SELECT regexp_replace('number   your     street,'||CHR(10)||'    zipcode  town, FR', '( ){2,}', ' ', 1, 0);
SELECT regexp_replace('number   your     street,    zipcode  town, FR', '( ){2,}', ' ', 9);
SELECT regexp_replace('number   your     street,    zipcode  town, FR', '( ){2,}', ' ', 9, 0);
SELECT regexp_replace('number   your     street,    zipcode  town, FR', '( ){2,}', ' ', 9, 2);
SELECT regexp_replace('number   your     street,    zipcode  town, FR', '( ){2,}', ' ', 9, 2, 'm');
SELECT regexp_replace('number   your     street,    zipcode  town, FR', '([EURT]){2,}', '[\1]', 9, 1, 'i');
SELECT regexp_replace('number   your     street,    zipcode  town, FR', '([EURT]){2,}', '[\1]', 9, 2, 'i');
SELECT regexp_replace ('A PostgreSQL function', 'A|e|i|o|u', 'X', 1, 2);
SELECT regexp_replace ('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 0, 'i');
SELECT regexp_replace ('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 1, 'i');
SELECT regexp_replace ('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 2, 'i');
SELECT regexp_replace ('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 3, 'i');
SELECT regexp_replace ('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 9, 'i');
SELECT regexp_replace ('A PostgreSQL function', 'A|e|i|o|u', 'X', 1, 9);
-- Invalid parameter values
SELECT regexp_replace ('A PostgreSQL function', 'a|e|i|o|u', 'X', -1, 0, 'i');
SELECT regexp_replace ('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, -1, 'i');
-- Modifier 'g' should not be taken in account, we have an occurrence to replace
SELECT regexp_replace ('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 1, 'g');
-- Find groups of "word characters" (letters, numbers and underscore) ending with
-- "thy" in the string "healthy, wealthy, and wise" and replace them with nothing.
SELECT regexp_replace('healthy, wealthy, and wise','\w+thy', '', 'g');
SELECT regexp_replace('healthy, wealthy, and wise','\w+thy', '', 1, 0);
SELECT regexp_replace('healthy, wealthy, and wise','\w+thy', '', 1, 0, 'g');
-- Find groups of word characters ending with "thy" and replace with "something."
SELECT regexp_replace('healthy, wealthy, and wise','\w+thy', 'something', 'g');
-- Find groups of word characters ending with "thy" and replace with the string
-- "something" starting at the third character in the string.
SELECT regexp_replace('healthy, wealthy, and wise','\w+thy', 'something', 3, 0);
-- Replace the second group of word characters ending with "thy" with "something."
SELECT regexp_replace('healthy, wealthy, and wise','\w+thy', 'something', 1, 2);
-- Find groups of word characters ending with "thy" capturing the letters before
-- the "thy", and replace with the captured letters plus the letters "ish." 
SELECT regexp_replace('healthy, wealthy, and wise','(\w+)thy', '\1ish', 'g');
SELECT regexp_replace('healthy, wealthy, and wise','(\w+)thy', '\1ish', 1, 0);


-- change NULL-display back
\pset null ''

-- E021-11 position expression
SELECT POSITION('4' IN '1234567890') = '4' AS "4";

SELECT POSITION('5' IN '1234567890') = '5' AS "5";

-- T312 character overlay function
SELECT OVERLAY('abcdef' PLACING '45' FROM 4) AS "abc45f";

SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5) AS "yabadaba";

SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5 FOR 0) AS "yabadabadoo";

SELECT OVERLAY('babosa' PLACING 'ubb' FROM 2 FOR 4) AS "bubba";

--
-- test LIKE
-- Be sure to form every test as a LIKE/NOT LIKE pair.
--

-- simplest examples
-- E061-04 like predicate
SELECT 'hawkeye' LIKE 'h%' AS "true";
SELECT 'hawkeye' NOT LIKE 'h%' AS "false";

SELECT 'hawkeye' LIKE 'H%' AS "false";
SELECT 'hawkeye' NOT LIKE 'H%' AS "true";

SELECT 'hawkeye' LIKE 'indio%' AS "false";
SELECT 'hawkeye' NOT LIKE 'indio%' AS "true";

SELECT 'hawkeye' LIKE 'h%eye' AS "true";
SELECT 'hawkeye' NOT LIKE 'h%eye' AS "false";

SELECT 'indio' LIKE '_ndio' AS "true";
SELECT 'indio' NOT LIKE '_ndio' AS "false";

SELECT 'indio' LIKE 'in__o' AS "true";
SELECT 'indio' NOT LIKE 'in__o' AS "false";

SELECT 'indio' LIKE 'in_o' AS "false";
SELECT 'indio' NOT LIKE 'in_o' AS "true";

SELECT 'abc'::name LIKE '_b_' AS "true";
SELECT 'abc'::name NOT LIKE '_b_' AS "false";

SELECT 'abc'::bytea LIKE '_b_'::bytea AS "true";
SELECT 'abc'::bytea NOT LIKE '_b_'::bytea AS "false";

-- unused escape character
SELECT 'hawkeye' LIKE 'h%' ESCAPE '#' AS "true";
SELECT 'hawkeye' NOT LIKE 'h%' ESCAPE '#' AS "false";

SELECT 'indio' LIKE 'ind_o' ESCAPE '$' AS "true";
SELECT 'indio' NOT LIKE 'ind_o' ESCAPE '$' AS "false";

-- escape character
-- E061-05 like predicate with escape clause
SELECT 'h%' LIKE 'h#%' ESCAPE '#' AS "true";
SELECT 'h%' NOT LIKE 'h#%' ESCAPE '#' AS "false";

SELECT 'h%wkeye' LIKE 'h#%' ESCAPE '#' AS "false";
SELECT 'h%wkeye' NOT LIKE 'h#%' ESCAPE '#' AS "true";

SELECT 'h%wkeye' LIKE 'h#%%' ESCAPE '#' AS "true";
SELECT 'h%wkeye' NOT LIKE 'h#%%' ESCAPE '#' AS "false";

SELECT 'h%awkeye' LIKE 'h#%a%k%e' ESCAPE '#' AS "true";
SELECT 'h%awkeye' NOT LIKE 'h#%a%k%e' ESCAPE '#' AS "false";

SELECT 'indio' LIKE '_ndio' ESCAPE '$' AS "true";
SELECT 'indio' NOT LIKE '_ndio' ESCAPE '$' AS "false";

SELECT 'i_dio' LIKE 'i$_d_o' ESCAPE '$' AS "true";
SELECT 'i_dio' NOT LIKE 'i$_d_o' ESCAPE '$' AS "false";

SELECT 'i_dio' LIKE 'i$_nd_o' ESCAPE '$' AS "false";
SELECT 'i_dio' NOT LIKE 'i$_nd_o' ESCAPE '$' AS "true";

SELECT 'i_dio' LIKE 'i$_d%o' ESCAPE '$' AS "true";
SELECT 'i_dio' NOT LIKE 'i$_d%o' ESCAPE '$' AS "false";

SELECT 'a_c'::bytea LIKE 'a$__'::bytea ESCAPE '$'::bytea AS "true";
SELECT 'a_c'::bytea NOT LIKE 'a$__'::bytea ESCAPE '$'::bytea AS "false";

-- escape character same as pattern character
SELECT 'maca' LIKE 'm%aca' ESCAPE '%' AS "true";
SELECT 'maca' NOT LIKE 'm%aca' ESCAPE '%' AS "false";

SELECT 'ma%a' LIKE 'm%a%%a' ESCAPE '%' AS "true";
SELECT 'ma%a' NOT LIKE 'm%a%%a' ESCAPE '%' AS "false";

SELECT 'bear' LIKE 'b_ear' ESCAPE '_' AS "true";
SELECT 'bear' NOT LIKE 'b_ear' ESCAPE '_' AS "false";

SELECT 'be_r' LIKE 'b_e__r' ESCAPE '_' AS "true";
SELECT 'be_r' NOT LIKE 'b_e__r' ESCAPE '_' AS "false";

SELECT 'be_r' LIKE '__e__r' ESCAPE '_' AS "false";
SELECT 'be_r' NOT LIKE '__e__r' ESCAPE '_' AS "true";


--
-- test ILIKE (case-insensitive LIKE)
-- Be sure to form every test as an ILIKE/NOT ILIKE pair.
--

SELECT 'hawkeye' ILIKE 'h%' AS "true";
SELECT 'hawkeye' NOT ILIKE 'h%' AS "false";

SELECT 'hawkeye' ILIKE 'H%' AS "true";
SELECT 'hawkeye' NOT ILIKE 'H%' AS "false";

SELECT 'hawkeye' ILIKE 'H%Eye' AS "true";
SELECT 'hawkeye' NOT ILIKE 'H%Eye' AS "false";

SELECT 'Hawkeye' ILIKE 'h%' AS "true";
SELECT 'Hawkeye' NOT ILIKE 'h%' AS "false";

SELECT 'ABC'::name ILIKE '_b_' AS "true";
SELECT 'ABC'::name NOT ILIKE '_b_' AS "false";

--
-- test %/_ combination cases, cf bugs #4821 and #5478
--

SELECT 'foo' LIKE '_%' as t, 'f' LIKE '_%' as t, '' LIKE '_%' as f;
SELECT 'foo' LIKE '%_' as t, 'f' LIKE '%_' as t, '' LIKE '%_' as f;

SELECT 'foo' LIKE '__%' as t, 'foo' LIKE '___%' as t, 'foo' LIKE '____%' as f;
SELECT 'foo' LIKE '%__' as t, 'foo' LIKE '%___' as t, 'foo' LIKE '%____' as f;

SELECT 'jack' LIKE '%____%' AS t;


--
-- basic tests of LIKE with indexes
--

CREATE TABLE texttest (a text PRIMARY KEY, b int);
SELECT * FROM texttest WHERE a LIKE '%1%';

CREATE TABLE byteatest (a bytea PRIMARY KEY, b int);
SELECT * FROM byteatest WHERE a LIKE '%1%';

DROP TABLE texttest, byteatest;


--
-- test implicit type conversion
--

-- E021-07 character concatenation
SELECT 'unknown' || ' and unknown' AS "Concat unknown types";

SELECT text 'text' || ' and unknown' AS "Concat text to unknown type";

SELECT char(20) 'characters' || ' and text' AS "Concat char to unknown type";

SELECT text 'text' || char(20) ' and characters' AS "Concat text to char";

SELECT text 'text' || varchar ' and varchar' AS "Concat text to varchar";

--
-- test substr with toasted text values
--
CREATE TABLE toasttest(f1 text);

insert into toasttest values(repeat('1234567890',10000));
insert into toasttest values(repeat('1234567890',10000));

--
-- Ensure that some values are uncompressed, to test the faster substring
-- operation used in that case
--
alter table toasttest alter column f1 set storage external;
insert into toasttest values(repeat('1234567890',10000));
insert into toasttest values(repeat('1234567890',10000));

-- If the starting position is zero or less, then return from the start of the string
-- adjusting the length to be consistent with the "negative start" per SQL.
SELECT substr(f1, -1, 5) from toasttest;

-- If the length is less than zero, an ERROR is thrown.
SELECT substr(f1, 5, -1) from toasttest;

-- If no third argument (length) is provided, the length to the end of the
-- string is assumed.
SELECT substr(f1, 99995) from toasttest;

-- If start plus length is > string length, the result is truncated to
-- string length
SELECT substr(f1, 99995, 10) from toasttest;

TRUNCATE TABLE toasttest;
INSERT INTO toasttest values (repeat('1234567890',300));
INSERT INTO toasttest values (repeat('1234567890',300));
INSERT INTO toasttest values (repeat('1234567890',300));
INSERT INTO toasttest values (repeat('1234567890',300));
-- expect >0 blocks
SELECT pg_relation_size(reltoastrelid) = 0 AS is_empty
  FROM pg_class where relname = 'toasttest';

TRUNCATE TABLE toasttest;
ALTER TABLE toasttest set (toast_tuple_target = 4080);
INSERT INTO toasttest values (repeat('1234567890',300));
INSERT INTO toasttest values (repeat('1234567890',300));
INSERT INTO toasttest values (repeat('1234567890',300));
INSERT INTO toasttest values (repeat('1234567890',300));
-- expect 0 blocks
SELECT pg_relation_size(reltoastrelid) = 0 AS is_empty
  FROM pg_class where relname = 'toasttest';

DROP TABLE toasttest;

--
-- test substr with toasted bytea values
--
CREATE TABLE toasttest(f1 bytea);

insert into toasttest values(decode(repeat('1234567890',10000),'escape'));
insert into toasttest values(decode(repeat('1234567890',10000),'escape'));

--
-- Ensure that some values are uncompressed, to test the faster substring
-- operation used in that case
--
alter table toasttest alter column f1 set storage external;
insert into toasttest values(decode(repeat('1234567890',10000),'escape'));
insert into toasttest values(decode(repeat('1234567890',10000),'escape'));

-- If the starting position is zero or less, then return from the start of the string
-- adjusting the length to be consistent with the "negative start" per SQL.
SELECT substr(f1, -1, 5) from toasttest;

-- If the length is less than zero, an ERROR is thrown.
SELECT substr(f1, 5, -1) from toasttest;

-- If no third argument (length) is provided, the length to the end of the
-- string is assumed.
SELECT substr(f1, 99995) from toasttest;

-- If start plus length is > string length, the result is truncated to
-- string length
SELECT substr(f1, 99995, 10) from toasttest;

DROP TABLE toasttest;

-- test internally compressing datums

-- this tests compressing a datum to a very small size which exercises a
-- corner case in packed-varlena handling: even though small, the compressed
-- datum must be given a 4-byte header because there are no bits to indicate
-- compression in a 1-byte header

CREATE TABLE toasttest (c char(4096));
INSERT INTO toasttest VALUES('x');
SELECT length(c), c::text FROM toasttest;
SELECT c FROM toasttest;
DROP TABLE toasttest;

--
-- test length
--

SELECT length('abcdef') AS "length_6";

--
-- test strpos
--

SELECT strpos('abcdef', 'cd') AS "pos_3";

SELECT strpos('abcdef', 'xy') AS "pos_0";

SELECT strpos('abcdef', '') AS "pos_1";

SELECT strpos('', 'xy') AS "pos_0";

SELECT strpos('', '') AS "pos_1";

--
-- test replace
--
SELECT replace('abcdef', 'de', '45') AS "abc45f";

SELECT replace('yabadabadoo', 'ba', '123') AS "ya123da123doo";

SELECT replace('yabadoo', 'bad', '') AS "yaoo";

--
-- test split_part
--
select split_part('','@',1) AS "empty string";

select split_part('','@',-1) AS "empty string";

select split_part('joeuser@mydatabase','',1) AS "joeuser@mydatabase";

select split_part('joeuser@mydatabase','',2) AS "empty string";

select split_part('joeuser@mydatabase','',-1) AS "joeuser@mydatabase";

select split_part('joeuser@mydatabase','',-2) AS "empty string";

select split_part('joeuser@mydatabase','@',0) AS "an error";

select split_part('joeuser@mydatabase','@@',1) AS "joeuser@mydatabase";

select split_part('joeuser@mydatabase','@@',2) AS "empty string";

select split_part('joeuser@mydatabase','@',1) AS "joeuser";

select split_part('joeuser@mydatabase','@',2) AS "mydatabase";

select split_part('joeuser@mydatabase','@',3) AS "empty string";

select split_part('@joeuser@mydatabase@','@',2) AS "joeuser";

select split_part('joeuser@mydatabase','@',-1) AS "mydatabase";

select split_part('joeuser@mydatabase','@',-2) AS "joeuser";

select split_part('joeuser@mydatabase','@',-3) AS "empty string";

select split_part('@joeuser@mydatabase@','@',-2) AS "mydatabase";

--
-- test to_hex
--
select to_hex(256*256*256 - 1) AS "ffffff";

select to_hex(256::bigint*256::bigint*256::bigint*256::bigint - 1) AS "ffffffff";

--
-- MD5 test suite - from IETF RFC 1321
-- (see: ftp://ftp.rfc-editor.org/in-notes/rfc1321.txt)
--
select md5('') = 'd41d8cd98f00b204e9800998ecf8427e' AS "TRUE";

select md5('a') = '0cc175b9c0f1b6a831c399e269772661' AS "TRUE";

select md5('abc') = '900150983cd24fb0d6963f7d28e17f72' AS "TRUE";

select md5('message digest') = 'f96b697d7cb7938d525a2f31aaf161d0' AS "TRUE";

select md5('abcdefghijklmnopqrstuvwxyz') = 'c3fcd3d76192e4007dfb496cca67e13b' AS "TRUE";

select md5('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789') = 'd174ab98d277d9f5a5611c2c9f419d9f' AS "TRUE";

select md5('12345678901234567890123456789012345678901234567890123456789012345678901234567890') = '57edf4a22be3c955ac49da2e2107b67a' AS "TRUE";

select md5(''::bytea) = 'd41d8cd98f00b204e9800998ecf8427e' AS "TRUE";

select md5('a'::bytea) = '0cc175b9c0f1b6a831c399e269772661' AS "TRUE";

select md5('abc'::bytea) = '900150983cd24fb0d6963f7d28e17f72' AS "TRUE";

select md5('message digest'::bytea) = 'f96b697d7cb7938d525a2f31aaf161d0' AS "TRUE";

select md5('abcdefghijklmnopqrstuvwxyz'::bytea) = 'c3fcd3d76192e4007dfb496cca67e13b' AS "TRUE";

select md5('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'::bytea) = 'd174ab98d277d9f5a5611c2c9f419d9f' AS "TRUE";

select md5('12345678901234567890123456789012345678901234567890123456789012345678901234567890'::bytea) = '57edf4a22be3c955ac49da2e2107b67a' AS "TRUE";

--
-- SHA-2
--
SET bytea_output TO hex;

SELECT sha224('');
SELECT sha224('The quick brown fox jumps over the lazy dog.');

SELECT sha256('');
SELECT sha256('The quick brown fox jumps over the lazy dog.');

SELECT sha384('');
SELECT sha384('The quick brown fox jumps over the lazy dog.');

SELECT sha512('');
SELECT sha512('The quick brown fox jumps over the lazy dog.');

--
-- encode/decode
--
SELECT encode('\x1234567890abcdef00', 'hex');
SELECT decode('1234567890abcdef00', 'hex');
SELECT encode(('\x' || repeat('1234567890abcdef0001', 7))::bytea, 'base64');
SELECT decode(encode(('\x' || repeat('1234567890abcdef0001', 7))::bytea,
                     'base64'), 'base64');
SELECT encode('\x1234567890abcdef00', 'escape');
SELECT decode(encode('\x1234567890abcdef00', 'escape'), 'escape');

--
-- get_bit/set_bit etc
--
SELECT get_bit('\x1234567890abcdef00'::bytea, 43);
SELECT get_bit('\x1234567890abcdef00'::bytea, 99);  -- error
SELECT set_bit('\x1234567890abcdef00'::bytea, 43, 0);
SELECT set_bit('\x1234567890abcdef00'::bytea, 99, 0);  -- error
SELECT get_byte('\x1234567890abcdef00'::bytea, 3);
SELECT get_byte('\x1234567890abcdef00'::bytea, 99);  -- error
SELECT set_byte('\x1234567890abcdef00'::bytea, 7, 11);
SELECT set_byte('\x1234567890abcdef00'::bytea, 99, 11);  -- error

--
-- test behavior of escape_string_warning and standard_conforming_strings options
--
set escape_string_warning = off;
set standard_conforming_strings = off;

show escape_string_warning;
show standard_conforming_strings;

set escape_string_warning = on;
set standard_conforming_strings = on;

show escape_string_warning;
show standard_conforming_strings;

select 'a\bcd' as f1, 'a\b''cd' as f2, 'a\b''''cd' as f3, 'abcd\'   as f4, 'ab\''cd' as f5, '\\' as f6;

set standard_conforming_strings = off;

select 'a\\bcd' as f1, 'a\\b\'cd' as f2, 'a\\b\'''cd' as f3, 'abcd\\'   as f4, 'ab\\\'cd' as f5, '\\\\' as f6;

set escape_string_warning = off;
set standard_conforming_strings = on;

select 'a\bcd' as f1, 'a\b''cd' as f2, 'a\b''''cd' as f3, 'abcd\'   as f4, 'ab\''cd' as f5, '\\' as f6;

set standard_conforming_strings = off;

select 'a\\bcd' as f1, 'a\\b\'cd' as f2, 'a\\b\'''cd' as f3, 'abcd\\'   as f4, 'ab\\\'cd' as f5, '\\\\' as f6;

reset standard_conforming_strings;


--
-- Additional string functions
--
SET bytea_output TO escape;

SELECT initcap('hi THOMAS');

SELECT lpad('hi', 5, 'xy');
SELECT lpad('hi', 5);
SELECT lpad('hi', -5, 'xy');
SELECT lpad('hello', 2);
SELECT lpad('hi', 5, '');

SELECT rpad('hi', 5, 'xy');
SELECT rpad('hi', 5);
SELECT rpad('hi', -5, 'xy');
SELECT rpad('hello', 2);
SELECT rpad('hi', 5, '');

SELECT ltrim('zzzytrim', 'xyz');

SELECT translate('', '14', 'ax');
SELECT translate('12345', '14', 'ax');

SELECT ascii('x');
SELECT ascii('');

SELECT chr(65);
SELECT chr(0);

SELECT repeat('Pg', 4);
SELECT repeat('Pg', -4);

SELECT SUBSTRING('1234567890'::bytea FROM 3) "34567890";
SELECT SUBSTRING('1234567890'::bytea FROM 4 FOR 3) AS "456";
SELECT SUBSTRING('string'::bytea FROM 2 FOR 2147483646) AS "tring";
SELECT SUBSTRING('string'::bytea FROM -10 FOR 2147483646) AS "string";
SELECT SUBSTRING('string'::bytea FROM -10 FOR -2147483646) AS "error";

SELECT trim(E'\\000'::bytea from E'\\000Tom\\000'::bytea);
SELECT trim(leading E'\\000'::bytea from E'\\000Tom\\000'::bytea);
SELECT trim(trailing E'\\000'::bytea from E'\\000Tom\\000'::bytea);
SELECT btrim(E'\\000trim\\000'::bytea, E'\\000'::bytea);
SELECT btrim(''::bytea, E'\\000'::bytea);
SELECT btrim(E'\\000trim\\000'::bytea, ''::bytea);
SELECT encode(overlay(E'Th\\000omas'::bytea placing E'Th\\001omas'::bytea from 2),'escape');
SELECT encode(overlay(E'Th\\000omas'::bytea placing E'\\002\\003'::bytea from 8),'escape');
SELECT encode(overlay(E'Th\\000omas'::bytea placing E'\\002\\003'::bytea from 5 for 3),'escape');

SELECT bit_count('\x1234567890'::bytea);

SELECT unistr('\0064at\+0000610');
SELECT unistr('d\u0061t\U000000610');
SELECT unistr('a\\b');
-- errors:
SELECT unistr('wrong: \db99');
SELECT unistr('wrong: \db99\0061');
SELECT unistr('wrong: \+00db99\+000061');
SELECT unistr('wrong: \+2FFFFF');
SELECT unistr('wrong: \udb99\u0061');
SELECT unistr('wrong: \U0000db99\U00000061');
SELECT unistr('wrong: \U002FFFFF');
SELECT unistr('wrong: \xyz');
