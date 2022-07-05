CREATE EXTENSION unaccent;

-- must have a UTF8 database
SELECT getdatabaseencoding();

SET client_encoding TO 'UTF8';

SELECT unaccent('foobar');
SELECT unaccent('ёлка');
SELECT unaccent('ЁЖИК');
SELECT unaccent('˃˖˗˜');
SELECT unaccent('À');  -- Remove combining diacritical 0x0300
SELECT unaccent('ℌ'); -- Controversial conversion from Latin-ASCII.xml
SELECT unaccent('Chrząszcza szczudłem przechrzcił wąż, Strząsa skrzydła z dżdżu,'); -- Polish

SELECT unaccent('unaccent', 'foobar');
SELECT unaccent('unaccent', 'ёлка');
SELECT unaccent('unaccent', 'ЁЖИК');
SELECT unaccent('unaccent', '˃˖˗˜');
SELECT unaccent('unaccent', 'À');
SELECT unaccent('unaccent', 'Łódź ąćęłńóśżź ĄĆĘŁŃÓŚŻŹ'); -- Polish letters

SELECT ts_lexize('unaccent', 'foobar');
SELECT ts_lexize('unaccent', 'ёлка');
SELECT ts_lexize('unaccent', 'ЁЖИК');
SELECT ts_lexize('unaccent', '˃˖˗˜');
SELECT ts_lexize('unaccent', 'À');

SELECT unaccent('unaccent', '⅓ ⅜ ℃ ℉ ℗');

SELECT unaccent('unaccent', '㎡ ㎥ ㎢ ㎣ ㎧');
