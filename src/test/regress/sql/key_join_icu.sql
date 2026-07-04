--
-- key_join_icu
--
-- FOR KEY join cases that depend on an ICU nondeterministic collation.  Such a
-- collation can only be created when the database encoding is one that ICU
-- supports (e.g. UTF8), so these cases used to live inline in key_join.sql and
-- key_join_mcdc.sql and broke the regression suite on SQL_ASCII databases and
-- on builds without ICU.  Move them here and skip the whole file unless an ICU
-- collation can actually be built; the rejection behaviour they exercise is
-- collation-mechanism specific and not otherwise reachable without ICU.
--

/* skip test if not UTF8 server encoding or no ICU collations installed */
SELECT getdatabaseencoding() <> 'UTF8' OR
       (SELECT count(*) FROM pg_collation WHERE collprovider = 'i' AND collname <> 'unicode') = 0
       AS skip_test \gset
\if :skip_test
\quit
\endif

CREATE SCHEMA key_join_icu;
SET search_path = key_join_icu, public;

-- Query-level DISTINCT over a nondeterministic collation cannot prove key
-- uniqueness (originally in key_join.sql).
CREATE COLLATION query_unique_nondet (provider = icu, locale = 'und',
    deterministic = false);
CREATE TABLE query_unique_nondet_parent
(
    id text COLLATE query_unique_nondet
);
CREATE TABLE query_unique_nondet_child
(
    parent_id text COLLATE query_unique_nondet
);
SELECT *
FROM (SELECT DISTINCT id FROM query_unique_nondet_parent) p
-- rejected, reason: nondeterministic DISTINCT collation cannot prove uniqueness
JOIN query_unique_nondet_child c FOR KEY (parent_id) -> p (id);
DROP TABLE query_unique_nondet_child, query_unique_nondet_parent;
DROP COLLATION query_unique_nondet;

-- GROUP BY over a nondeterministic-collation key projects no key facts, so the
-- key join cannot be proven (originally in key_join_mcdc.sql, which runs with
-- terse error verbosity).
\set VERBOSITY terse
CREATE COLLATION mcdc_nondet (provider = icu, locale = 'und',
    deterministic = false);

CREATE TABLE mcdc_text_parent
(
    id text COLLATE mcdc_nondet PRIMARY KEY
);

CREATE TABLE mcdc_text_reader
(
    id        int PRIMARY KEY,
    parent_id text COLLATE mcdc_nondet NOT NULL
        REFERENCES mcdc_text_parent (id)
);

INSERT INTO mcdc_text_parent VALUES ('a'), ('b');
INSERT INTO mcdc_text_reader VALUES (301, 'a'), (302, 'b');

SELECT tr.id, g.id
FROM (
    SELECT p.id
    FROM mcdc_text_parent p
    GROUP BY p.id
) g
JOIN mcdc_text_reader tr FOR KEY (parent_id) -> g (id)
ORDER BY tr.id;

DROP TABLE mcdc_text_reader, mcdc_text_parent;
DROP COLLATION mcdc_nondet;
\set VERBOSITY default

DROP SCHEMA key_join_icu;
