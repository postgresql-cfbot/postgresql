CREATE EXTENSION ltree;

SET standard_conforming_strings=on;

-- Check whether any of our opclasses fail amvalidate
SELECT amname, opcname
FROM pg_opclass opc LEFT JOIN pg_am am ON am.oid = opcmethod
WHERE opc.oid >= 16384 AND NOT amvalidate(opc.oid);

SELECT ''::ltree;
SELECT '1'::ltree;
SELECT '1.2'::ltree;
SELECT '1.2._3'::ltree;

SELECT ltree2text('1.2.3.34.sdf');
SELECT text2ltree('1.2.3.34.sdf');

SELECT subltree('Top.Child1.Child2',1,2);
SELECT subpath('Top.Child1.Child2',1,2);
SELECT subpath('Top.Child1.Child2',-1,1);
SELECT subpath('Top.Child1.Child2',0,-2);
SELECT subpath('Top.Child1.Child2',0,-1);
SELECT subpath('Top.Child1.Child2',0,0);
SELECT subpath('Top.Child1.Child2',1,0);
SELECT subpath('Top.Child1.Child2',0);
SELECT subpath('Top.Child1.Child2',1);


SELECT index('1.2.3.4.5.6','1.2');
SELECT index('a.1.2.3.4.5.6','1.2');
SELECT index('a.1.2.3.4.5.6','1.2.3');
SELECT index('a.1.2.3.4.5.6','1.2.3.j');
SELECT index('a.1.2.3.4.5.6','1.2.3.j.4.5.5.5.5.5.5');
SELECT index('a.1.2.3.4.5.6','1.2.3');
SELECT index('a.1.2.3.4.5.6','6');
SELECT index('a.1.2.3.4.5.6','6.1');
SELECT index('a.1.2.3.4.5.6','5.6');
SELECT index('0.1.2.3.5.4.5.6','5.6');
SELECT index('0.1.2.3.5.4.5.6.8.5.6.8','5.6',3);
SELECT index('0.1.2.3.5.4.5.6.8.5.6.8','5.6',6);
SELECT index('0.1.2.3.5.4.5.6.8.5.6.8','5.6',7);
SELECT index('0.1.2.3.5.4.5.6.8.5.6.8','5.6',-7);
SELECT index('0.1.2.3.5.4.5.6.8.5.6.8','5.6',-4);
SELECT index('0.1.2.3.5.4.5.6.8.5.6.8','5.6',-3);
SELECT index('0.1.2.3.5.4.5.6.8.5.6.8','5.6',-2);
SELECT index('0.1.2.3.5.4.5.6.8.5.6.8','5.6',-20000);


SELECT 'Top.Child1.Child2'::ltree || 'Child3'::text;
SELECT 'Top.Child1.Child2'::ltree || 'Child3'::ltree;
SELECT 'Top_0'::ltree || 'Top.Child1.Child2'::ltree;
SELECT 'Top.Child1.Child2'::ltree || ''::ltree;
SELECT ''::ltree || 'Top.Child1.Child2'::ltree;

SELECT lca('{la.2.3,1.2.3.4.5.6,""}') IS NULL;
SELECT lca('{la.2.3,1.2.3.4.5.6}') IS NULL;
SELECT lca('{1.la.2.3,1.2.3.4.5.6}');
SELECT lca('{1.2.3,1.2.3.4.5.6}');
SELECT lca('{1.2.3}');
SELECT lca('{1}'), lca('{1}') IS NULL;
SELECT lca('{}') IS NULL;
SELECT lca('1.la.2.3','1.2.3.4.5.6');
SELECT lca('1.2.3','1.2.3.4.5.6');
SELECT lca('1.2.2.3','1.2.3.4.5.6');
SELECT lca('1.2.2.3','1.2.3.4.5.6','');
SELECT lca('1.2.2.3','1.2.3.4.5.6','2');
SELECT lca('1.2.2.3','1.2.3.4.5.6','1');


SELECT '1'::lquery;
SELECT '4|3|2'::lquery;
SELECT '1.2'::lquery;
SELECT '1.4|3|2'::lquery;
SELECT '1.0'::lquery;
SELECT '4|3|2.0'::lquery;
SELECT '1.2.0'::lquery;
SELECT '1.4|3|2.0'::lquery;
SELECT '1.*'::lquery;
SELECT '4|3|2.*'::lquery;
SELECT '1.2.*'::lquery;
SELECT '1.4|3|2.*'::lquery;
SELECT '*.1.*'::lquery;
SELECT '*.4|3|2.*'::lquery;
SELECT '*.1.2.*'::lquery;
SELECT '*.1.4|3|2.*'::lquery;
SELECT '1.*.4|3|2'::lquery;
SELECT '1.*.4|3|2.0'::lquery;
SELECT '1.*.4|3|2.*{1,4}'::lquery;
SELECT '1.*.4|3|2.*{,4}'::lquery;
SELECT '1.*.4|3|2.*{1,}'::lquery;
SELECT '1.*.4|3|2.*{1}'::lquery;
SELECT 'qwerty%@*.tu'::lquery;

SELECT nlevel('1.2.3.4');
SELECT '1.2'::ltree  < '2.2'::ltree;
SELECT '1.2'::ltree  <= '2.2'::ltree;
SELECT '2.2'::ltree  = '2.2'::ltree;
SELECT '3.2'::ltree  >= '2.2'::ltree;
SELECT '3.2'::ltree  > '2.2'::ltree;

SELECT '1.2.3'::ltree @> '1.2.3.4'::ltree;
SELECT '1.2.3.4'::ltree @> '1.2.3.4'::ltree;
SELECT '1.2.3.4.5'::ltree @> '1.2.3.4'::ltree;
SELECT '1.3.3'::ltree @> '1.2.3.4'::ltree;

SELECT 'a.b.c.d.e'::ltree ~ 'a.b.c.d.e';
SELECT 'a.b.c.d.e'::ltree ~ 'A.b.c.d.e';
SELECT 'a.b.c.d.e'::ltree ~ 'A@.b.c.d.e';
SELECT 'aa.b.c.d.e'::ltree ~ 'A@.b.c.d.e';
SELECT 'aa.b.c.d.e'::ltree ~ 'A*.b.c.d.e';
SELECT 'aa.b.c.d.e'::ltree ~ 'A*@.b.c.d.e';
SELECT 'aa.b.c.d.e'::ltree ~ 'A*@|g.b.c.d.e';
SELECT 'g.b.c.d.e'::ltree ~ 'A*@|g.b.c.d.e';
SELECT 'a.b.c.d.e'::ltree ~ 'a.b.c.d.e';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*.e';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*{3}.e';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*{2}.e';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*{4}.e';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*{,4}.e';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*{2,}.e';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*{2,4}.e';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*{2,3}.e';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*{2,3}';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*{2,4}';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*{2,5}';
SELECT 'a.b.c.d.e'::ltree ~ '*{2,3}.e';
SELECT 'a.b.c.d.e'::ltree ~ '*{2,4}.e';
SELECT 'a.b.c.d.e'::ltree ~ '*{2,5}.e';
SELECT 'a.b.c.d.e'::ltree ~ '*.e';
SELECT 'a.b.c.d.e'::ltree ~ '*.e.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.d.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.a.*.d.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.!d.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.!d';
SELECT 'a.b.c.d.e'::ltree ~ '!d.*';
SELECT 'a.b.c.d.e'::ltree ~ '!a.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.!e';
SELECT 'a.b.c.d.e'::ltree ~ '*.!e.*';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*.!e';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*.!d';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*.!d.*';
SELECT 'a.b.c.d.e'::ltree ~ 'a.*.!f.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.a.*.!f.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.a.*.!d.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.a.!d.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.a.!d';
SELECT 'a.b.c.d.e'::ltree ~ 'a.!d.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.a.*.!d.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.!b.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.!b.c.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.!b.*.c.*';
SELECT 'a.b.c.d.e'::ltree ~ '!b.*.c.*';
SELECT 'a.b.c.d.e'::ltree ~ '!b.b.*';
SELECT 'a.b.c.d.e'::ltree ~ '!b.*.e';
SELECT 'a.b.c.d.e'::ltree ~ '!b.!c.*.e';
SELECT 'a.b.c.d.e'::ltree ~ '!b.*.!c.*.e';
SELECT 'a.b.c.d.e'::ltree ~ '*{2}.!b.*.!c.*.e';
SELECT 'a.b.c.d.e'::ltree ~ '*{1}.!b.*.!c.*.e';
SELECT 'a.b.c.d.e'::ltree ~ '*{1}.!b.*{1}.!c.*.e';
SELECT 'a.b.c.d.e'::ltree ~ 'a.!b.*{1}.!c.*.e';
SELECT 'a.b.c.d.e'::ltree ~ '!b.*{1}.!c.*.e';
SELECT 'a.b.c.d.e'::ltree ~ '*.!b.*{1}.!c.*.e';
SELECT 'a.b.c.d.e'::ltree ~ '*.!b.*.!c.*.e';
SELECT 'a.b.c.d.e'::ltree ~ '!b.!c.*';
SELECT 'a.b.c.d.e'::ltree ~ '!b.*.!c.*';
SELECT 'a.b.c.d.e'::ltree ~ '*{2}.!b.*.!c.*';
SELECT 'a.b.c.d.e'::ltree ~ '*{1}.!b.*.!c.*';
SELECT 'a.b.c.d.e'::ltree ~ '*{1}.!b.*{1}.!c.*';
SELECT 'a.b.c.d.e'::ltree ~ 'a.!b.*{1}.!c.*';
SELECT 'a.b.c.d.e'::ltree ~ '!b.*{1}.!c.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.!b.*{1}.!c.*';
SELECT 'a.b.c.d.e'::ltree ~ '*.!b.*.!c.*';


SELECT 'QWER_TY'::ltree ~ 'q%@*';
SELECT 'QWER_TY'::ltree ~ 'Q_t%@*';
SELECT 'QWER_GY'::ltree ~ 'q_t%@*';

--ltxtquery
SELECT '!tree & aWdf@*'::ltxtquery;
SELECT 'tree & aw_qw%*'::ltxtquery;
SELECT 'ltree.awdfg'::ltree @ '!tree & aWdf@*'::ltxtquery;
SELECT 'tree.awdfg'::ltree @ '!tree & aWdf@*'::ltxtquery;
SELECT 'tree.awdfg'::ltree @ '!tree | aWdf@*'::ltxtquery;
SELECT 'tree.awdfg'::ltree @ 'tree | aWdf@*'::ltxtquery;
SELECT 'tree.awdfg'::ltree @ 'tree & aWdf@*'::ltxtquery;
SELECT 'tree.awdfg'::ltree @ 'tree & aWdf@'::ltxtquery;
SELECT 'tree.awdfg'::ltree @ 'tree & aWdf*'::ltxtquery;
SELECT 'tree.awdfg'::ltree @ 'tree & aWdf'::ltxtquery;
SELECT 'tree.awdfg'::ltree @ 'tree & awdf*'::ltxtquery;
SELECT 'tree.awdfg'::ltree @ 'tree & aWdfg@'::ltxtquery;
SELECT 'tree.awdfg_qwerty'::ltree @ 'tree & aw_qw%*'::ltxtquery;
SELECT 'tree.awdfg_qwerty'::ltree @ 'tree & aw_rw%*'::ltxtquery;

--arrays

SELECT '{1.2.3}'::ltree[] @> '1.2.3.4';
SELECT '{1.2.3.4}'::ltree[] @> '1.2.3.4';
SELECT '{1.2.3.4.5}'::ltree[] @> '1.2.3.4';
SELECT '{1.3.3}'::ltree[] @> '1.2.3.4';
SELECT '{5.67.8, 1.2.3}'::ltree[] @> '1.2.3.4';
SELECT '{5.67.8, 1.2.3.4}'::ltree[] @> '1.2.3.4';
SELECT '{5.67.8, 1.2.3.4.5}'::ltree[] @> '1.2.3.4';
SELECT '{5.67.8, 1.3.3}'::ltree[] @> '1.2.3.4';
SELECT '{1.2.3, 7.12.asd}'::ltree[] @> '1.2.3.4';
SELECT '{1.2.3.4, 7.12.asd}'::ltree[] @> '1.2.3.4';
SELECT '{1.2.3.4.5, 7.12.asd}'::ltree[] @> '1.2.3.4';
SELECT '{1.3.3, 7.12.asd}'::ltree[] @> '1.2.3.4';
SELECT '{ltree.asd, tree.awdfg}'::ltree[] @ 'tree & aWdfg@'::ltxtquery;
SELECT '{j.k.l.m, g.b.c.d.e}'::ltree[] ~ 'A*@|g.b.c.d.e';
SELECT 'a.b.c.d.e'::ltree ? '{A.b.c.d.e}';
SELECT 'a.b.c.d.e'::ltree ? '{a.b.c.d.e}';
SELECT 'a.b.c.d.e'::ltree ? '{A.b.c.d.e, a.*}';
SELECT '{a.b.c.d.e,B.df}'::ltree[] ? '{A.b.c.d.e}';
SELECT '{a.b.c.d.e,B.df}'::ltree[] ? '{A.b.c.d.e,*.df}';

--extractors
SELECT ('{3456,1.2.3.34}'::ltree[] ?@> '1.2.3.4') is null;
SELECT '{3456,1.2.3}'::ltree[] ?@> '1.2.3.4';
SELECT '{3456,1.2.3.4}'::ltree[] ?<@ '1.2.3';
SELECT ('{3456,1.2.3.4}'::ltree[] ?<@ '1.2.5') is null;
SELECT '{ltree.asd, tree.awdfg}'::ltree[] ?@ 'tree & aWdfg@'::ltxtquery;
SELECT '{j.k.l.m, g.b.c.d.e}'::ltree[] ?~ 'A*@|g.b.c.d.e';

CREATE TABLE ltreetest (t ltree);
\copy ltreetest FROM 'data/ltree.data'

SELECT * FROM ltreetest WHERE t <  '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t <= '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t =  '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t >= '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t >  '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t @> '1.1.1' order by t asc;
SELECT * FROM ltreetest WHERE t <@ '1.1.1' order by t asc;
SELECT * FROM ltreetest WHERE t @ '23 & 1' order by t asc;
SELECT * FROM ltreetest WHERE t ~ '1.1.1.*' order by t asc;
SELECT * FROM ltreetest WHERE t ~ '*.1' order by t asc;
SELECT * FROM ltreetest WHERE t ~ '23.*{1}.1' order by t asc;
SELECT * FROM ltreetest WHERE t ~ '23.*.1' order by t asc;
SELECT * FROM ltreetest WHERE t ~ '23.*.2' order by t asc;
SELECT * FROM ltreetest WHERE t ? '{23.*.1,23.*.2}' order by t asc;

create unique index tstidx on ltreetest (t);
set enable_seqscan=off;

SELECT * FROM ltreetest WHERE t <  '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t <= '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t =  '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t >= '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t >  '12.3' order by t asc;

drop index tstidx;
create index tstidx on ltreetest using gist (t);
set enable_seqscan=off;

SELECT * FROM ltreetest WHERE t <  '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t <= '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t =  '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t >= '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t >  '12.3' order by t asc;
SELECT * FROM ltreetest WHERE t @> '1.1.1' order by t asc;
SELECT * FROM ltreetest WHERE t <@ '1.1.1' order by t asc;
SELECT * FROM ltreetest WHERE t @ '23 & 1' order by t asc;
SELECT * FROM ltreetest WHERE t ~ '1.1.1.*' order by t asc;
SELECT * FROM ltreetest WHERE t ~ '*.1' order by t asc;
SELECT * FROM ltreetest WHERE t ~ '23.*{1}.1' order by t asc;
SELECT * FROM ltreetest WHERE t ~ '23.*.1' order by t asc;
SELECT * FROM ltreetest WHERE t ~ '23.*.2' order by t asc;
SELECT * FROM ltreetest WHERE t ? '{23.*.1,23.*.2}' order by t asc;

create table _ltreetest (t ltree[]);
\copy _ltreetest FROM 'data/_ltree.data'

SELECT count(*) FROM _ltreetest WHERE t @> '1.1.1' ;
SELECT count(*) FROM _ltreetest WHERE t <@ '1.1.1' ;
SELECT count(*) FROM _ltreetest WHERE t @ '23 & 1' ;
SELECT count(*) FROM _ltreetest WHERE t ~ '1.1.1.*' ;
SELECT count(*) FROM _ltreetest WHERE t ~ '*.1' ;
SELECT count(*) FROM _ltreetest WHERE t ~ '23.*{1}.1' ;
SELECT count(*) FROM _ltreetest WHERE t ~ '23.*.1' ;
SELECT count(*) FROM _ltreetest WHERE t ~ '23.*.2' ;
SELECT count(*) FROM _ltreetest WHERE t ? '{23.*.1,23.*.2}' ;

create index _tstidx on _ltreetest using gist (t);
set enable_seqscan=off;

SELECT count(*) FROM _ltreetest WHERE t @> '1.1.1' ;
SELECT count(*) FROM _ltreetest WHERE t <@ '1.1.1' ;
SELECT count(*) FROM _ltreetest WHERE t @ '23 & 1' ;
SELECT count(*) FROM _ltreetest WHERE t ~ '1.1.1.*' ;
SELECT count(*) FROM _ltreetest WHERE t ~ '*.1' ;
SELECT count(*) FROM _ltreetest WHERE t ~ '23.*{1}.1' ;
SELECT count(*) FROM _ltreetest WHERE t ~ '23.*.1' ;
SELECT count(*) FROM _ltreetest WHERE t ~ '23.*.2' ;
SELECT count(*) FROM _ltreetest WHERE t ? '{23.*.1,23.*.2}' ;

-- Extended syntax, escaping, quoting etc
-- success
SELECT E'\\.'::ltree;
SELECT E'\\ '::ltree;
SELECT E'\\\\'::ltree;
SELECT E'\\a'::ltree;
SELECT E'\\n'::ltree;
SELECT E'x\\\\'::ltree;
SELECT E'x\\ '::ltree;
SELECT E'x\\.'::ltree;
SELECT E'x\\a'::ltree;
SELECT E'x\\n'::ltree;
SELECT 'a b.с d'::ltree;
SELECT ' e . f '::ltree;
SELECT ' '::ltree;

SELECT E'\\ g  . h\\ '::ltree;
SELECT E'\\ g'::ltree;
SELECT E' h\\ '::ltree;
SELECT '" g  "." h "'::ltree;
SELECT '" g  " '::ltree;
SELECT '" g  "   ." h "  '::ltree;

SELECT nlevel(E'Bottom\\.Test'::ltree);
SELECT subpath(E'Bottom\\.'::ltree, 0, 1);

SELECT subpath(E'a\\.b', 0, 1);
SELECT subpath(E'a\\..b', 1, 1);
SELECT subpath(E'a\\..\\b', 1, 1);
SELECT subpath(E'a b.с d'::ltree, 1, 1);

SELECT(
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'\z\z\z\z\z')::ltree;

SELECT('   ' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'\a\b\c\d\e   ')::ltree;

SELECT 'abc\|d'::lquery;
SELECT 'abc\|d'::ltree ~ 'abc\|d'::lquery;
SELECT 'abc|d'::ltree ~ 'abc*'::lquery; --true
SELECT 'abc|d'::ltree ~ 'abc\*'::lquery; --false
SELECT E'abc|\\.'::ltree ~ 'abc\|*'::lquery; --true

SELECT E'"\\""'::ltree;
SELECT '\"'::ltree;
SELECT E'\\"'::ltree;
SELECT 'a\"b'::ltree;
SELECT '"ab"'::ltree;
SELECT '"."'::ltree;
SELECT E'".\\""'::ltree;
SELECT(
'"01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'\z\z\z\z\z"')::ltree;

SELECT E'"\\""'::lquery;
SELECT '\"'::lquery;
SELECT E'\\"'::lquery;
SELECT 'a\"b'::lquery;
SELECT '"ab"'::lquery;
SELECT '"."'::lquery;
SELECT E'".\\""'::lquery;
SELECT(
'"01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'\z\z\z\z\z"')::lquery;

SELECT ' e . f '::lquery;
SELECT ' e | f '::lquery;

SELECT E'\\ g  . h\\ '::lquery;
SELECT E'\\ g'::lquery;
SELECT E' h\\ '::lquery;
SELECT E'"\\ g"'::lquery;
SELECT E' "h\\ "'::lquery;
SELECT '" g  "." h "'::lquery;

SELECT E'\\ g  | h\\ '::lquery;
SELECT '" g  "|" h "'::lquery;

SELECT '" g  " '::lquery;
SELECT '" g  "    ." h "  '::lquery;
SELECT '" g  "    |  " h "   '::lquery;

SELECT('   ' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'\a\b\c\d\e   ')::lquery;

SELECT E'"a\\"b"'::lquery;
SELECT '"a!b"'::lquery;
SELECT '"a%b"'::lquery;
SELECT '"a*b"'::lquery;
SELECT '"a@b"'::lquery;
SELECT '"a{b"'::lquery;
SELECT '"a}b"'::lquery;
SELECT '"a|b"'::lquery;

SELECT E'a\\"b'::lquery;
SELECT E'a\\!b'::lquery;
SELECT E'a\\%b'::lquery;
SELECT E'a\\*b'::lquery;
SELECT E'a\\@b'::lquery;
SELECT E'a\\{b'::lquery;
SELECT E'a\\}b'::lquery;
SELECT E'a\\|b'::lquery;

SELECT '!"!b"'::lquery;
SELECT '!"%b"'::lquery;
SELECT '!"*b"'::lquery;
SELECT '!"@b"'::lquery;
SELECT '!"{b"'::lquery;
SELECT '!"}b"'::lquery;

SELECT E'!\\!b'::lquery;
SELECT E'!\\%b'::lquery;
SELECT E'!\\*b'::lquery;
SELECT E'!\\@b'::lquery;
SELECT E'!\\{b'::lquery;
SELECT E'!\\}b'::lquery;

SELECT '"1"'::lquery;
SELECT '"2.*"'::lquery;
SELECT '!"1"'::lquery;
SELECT '!"1|"'::lquery;
SELECT '4|3|"2"'::lquery;
SELECT '"1".2'::lquery;
SELECT '"1.4"|"3"|2'::lquery;
SELECT '"1"."4"|"3"|"2"'::lquery;
SELECT '"1"."0"'::lquery;
SELECT '"1".0'::lquery;
SELECT '"1".*'::lquery;
SELECT '4|"3"|2.*'::lquery;
SELECT '4|"3"|"2.*"'::lquery;
SELECT '2."*"'::lquery;
SELECT '"*".1."*"'::lquery;
SELECT '"*.4"|3|2.*'::lquery;
SELECT '"*.4"|3|"2.*"'::lquery;
SELECT '1.*.4|3|2.*{,4}'::lquery;
SELECT '1.*.4|3|2.*{1,}'::lquery;
SELECT '1.*.4|3|2.*{1}'::lquery;
SELECT '"qwerty"%@*.tu'::lquery;

SELECT '1.*.4|3|"2".*{1,4}'::lquery;
SELECT '1."*".4|3|"2".*{1,4}'::lquery;
SELECT '\% \@'::lquery;
SELECT '"\% \@"'::lquery;

SELECT E'\\aa.b.c.d.e'::ltree ~ 'A@.b.c.d.e';
SELECT E'a\\a.b.c.\\d.e'::ltree ~ 'A*.b.c.d.e';
SELECT E'a\\a.b.c.\\d.e'::ltree ~ E'A*@.b.c.d.\\e';
SELECT E'a\\a.b.c.\\d.e'::ltree ~ E'A*@|\\g.b.c.d.e';
--ltxtquery
SELECT '!"tree" & aWdf@*'::ltxtquery;
SELECT '"!tree" & aWdf@*'::ltxtquery;
SELECT E'tr\\ee'::ltree @ E'\\t\\r\\e\\e'::ltxtquery;
SELECT E'tr\\ee.awd\\fg'::ltree @ E'tre\\e & a\\Wdf@*'::ltxtquery;
SELECT 'tree & aw_qw%*'::ltxtquery;
SELECT 'tree."awdfg"'::ltree @ E'tree & a\\Wdf@*'::ltxtquery;
SELECT 'tree."awdfg"'::ltree @ E'tree & "a\\Wdf"@*'::ltxtquery;
SELECT 'tree.awdfg_qwerty'::ltree @ 'tree & aw_qw%*'::ltxtquery;
SELECT 'tree.awdfg_qwerty'::ltree @ 'tree & "aw_rw"%*'::ltxtquery;
SELECT 'tree.awdfg_qwerty'::ltree @ E'tree & "aw\\_qw"%*'::ltxtquery;
SELECT 'tree.awdfg_qwerty'::ltree @ E'tree & aw\\_qw%*'::ltxtquery;

SELECT E'"a\\"b"'::ltxtquery;
SELECT '"a!b"'::ltxtquery;
SELECT '"a%b"'::ltxtquery;
SELECT '"a*b"'::ltxtquery;
SELECT '"a@b"'::ltxtquery;
SELECT '"a{b"'::ltxtquery;
SELECT '"a}b"'::ltxtquery;
SELECT '"a|b"'::ltxtquery;
SELECT '"a&b"'::ltxtquery;
SELECT '"a(b"'::ltxtquery;
SELECT '"a)b"'::ltxtquery;

SELECT E'a\\"b'::ltxtquery;
SELECT E'a\\!b'::ltxtquery;
SELECT E'a\\%b'::ltxtquery;
SELECT E'a\\*b'::ltxtquery;
SELECT E'a\\@b'::ltxtquery;
SELECT E'a\\{b'::ltxtquery;
SELECT E'a\\}b'::ltxtquery;
SELECT E'a\\|b'::ltxtquery;
SELECT E'a\\&b'::ltxtquery;
SELECT E'a\\(b'::ltxtquery;
SELECT E'a\\)b'::ltxtquery;

SELECT E'"\\"b"'::ltxtquery;
SELECT '"!b"'::ltxtquery;
SELECT '"%b"'::ltxtquery;
SELECT '"*b"'::ltxtquery;
SELECT '"@b"'::ltxtquery;
SELECT '"{b"'::ltxtquery;
SELECT '"}b"'::ltxtquery;
SELECT '"|b"'::ltxtquery;
SELECT '"&b"'::ltxtquery;
SELECT '"(b"'::ltxtquery;
SELECT '")b"'::ltxtquery;

SELECT E'\\"b'::ltxtquery;
SELECT E'\\!b'::ltxtquery;
SELECT E'\\%b'::ltxtquery;
SELECT E'\\*b'::ltxtquery;
SELECT E'\\@b'::ltxtquery;
SELECT E'\\{b'::ltxtquery;
SELECT E'\\}b'::ltxtquery;
SELECT E'\\|b'::ltxtquery;
SELECT E'\\&b'::ltxtquery;
SELECT E'\\(b'::ltxtquery;
SELECT E'\\)b'::ltxtquery;

SELECT E'"a\\""'::ltxtquery;
SELECT '"a!"'::ltxtquery;
SELECT '"a%"'::ltxtquery;
SELECT '"a*"'::ltxtquery;
SELECT '"a@"'::ltxtquery;
SELECT '"a{"'::ltxtquery;
SELECT '"a}"'::ltxtquery;
SELECT '"a|"'::ltxtquery;
SELECT '"a&"'::ltxtquery;
SELECT '"a("'::ltxtquery;
SELECT '"a)"'::ltxtquery;

SELECT E'a\\"'::ltxtquery;
SELECT E'a\\!'::ltxtquery;
SELECT E'a\\%'::ltxtquery;
SELECT E'a\\*'::ltxtquery;
SELECT E'a\\@'::ltxtquery;
SELECT E'a\\{'::ltxtquery;
SELECT E'a\\}'::ltxtquery;
SELECT E'a\\|'::ltxtquery;
SELECT E'a\\&'::ltxtquery;
SELECT E'a\\('::ltxtquery;
SELECT E'a\\)'::ltxtquery;

--failures
SELECT E'\\'::ltree;
SELECT E'n\\'::ltree;
SELECT '"'::ltree;
SELECT '"a'::ltree;
SELECT '""'::ltree;
SELECT 'a"b'::ltree;
SELECT E'\\"ab"'::ltree;
SELECT '"a"."a'::ltree;
SELECT '"a."a"'::ltree;
SELECT '"".a'::ltree;
SELECT 'a.""'::ltree;
SELECT '"".""'::ltree;
SELECT '""'::lquery;
SELECT '"".""'::lquery;
SELECT 'a.""'::lquery;
SELECT ' . '::ltree;
SELECT ' . '::lquery;
SELECT ' | '::lquery;

SELECT(
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'z\z\z\z\z\z')::ltree;
SELECT(
'"01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'\z\z\z\z\z\z"')::ltree;

SELECT '"'::lquery;
SELECT '"a'::lquery;
SELECT '"a"."a'::lquery;
SELECT '"a."a"'::lquery;

SELECT E'\\"ab"'::lquery;
SELECT 'a"b'::lquery;
SELECT 'a!b'::lquery;
SELECT 'a%b'::lquery;
SELECT 'a*b'::lquery;
SELECT 'a@b'::lquery;
SELECT 'a{b'::lquery;
SELECT 'a}b'::lquery;

SELECT 'a!'::lquery;
SELECT 'a{'::lquery;
SELECT 'a}'::lquery;

SELECT '%b'::lquery;
SELECT '*b'::lquery;
SELECT '@b'::lquery;
SELECT '{b'::lquery;
SELECT '}b'::lquery;

SELECT '!%b'::lquery;
SELECT '!*b'::lquery;
SELECT '!@b'::lquery;
SELECT '!{b'::lquery;
SELECT '!}b'::lquery;

SELECT '"qwert"y.tu'::lquery;
SELECT 'q"wert"y"%@*.tu'::lquery;

SELECT(
'"01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'01234567890123456789012345678901234567890123456789' ||
'\z\z\z\z\z\z"')::lquery;

SELECT 'a | ""'::ltxtquery;
SELECT '"" & ""'::ltxtquery;
SELECT 'a.""'::ltxtquery;
SELECT '"'::ltxtquery;

SELECT '"""'::ltxtquery;
SELECT '"a'::ltxtquery;
SELECT '"a" & "a'::ltxtquery;
SELECT '"a | "a"'::ltxtquery;
SELECT '"!tree" & aWdf@*"'::ltxtquery;

SELECT 'a"b'::ltxtquery;
SELECT 'a!b'::ltxtquery;
SELECT 'a%b'::ltxtquery;
SELECT 'a*b'::ltxtquery;
SELECT 'a@b'::ltxtquery;
SELECT 'a{b'::ltxtquery;
SELECT 'a}b'::ltxtquery;
SELECT 'a|b'::ltxtquery;
SELECT 'a&b'::ltxtquery;
SELECT 'a(b'::ltxtquery;
SELECT 'a)b'::ltxtquery;

SELECT '"b'::ltxtquery;
SELECT '%b'::ltxtquery;
SELECT '*b'::ltxtquery;
SELECT '@b'::ltxtquery;
SELECT '{b'::ltxtquery;
SELECT '}b'::ltxtquery;
SELECT '|b'::ltxtquery;
SELECT '&b'::ltxtquery;
SELECT '(b'::ltxtquery;
SELECT ')b'::ltxtquery;

SELECT 'a"'::ltxtquery;
SELECT 'a!'::ltxtquery;
SELECT 'a{'::ltxtquery;
SELECT 'a}'::ltxtquery;
SELECT 'a|'::ltxtquery;
SELECT 'a&'::ltxtquery;
SELECT 'a('::ltxtquery;
SELECT 'a)'::ltxtquery;

