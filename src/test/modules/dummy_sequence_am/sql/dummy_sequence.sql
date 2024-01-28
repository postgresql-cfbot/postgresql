CREATE EXTENSION dummy_sequence_am;

CREATE SEQUENCE dummyseq USING dummy_sequence_am;

SELECT nextval('dummyseq'::regclass);
SELECT setval('dummyseq'::regclass, 14);
SELECT nextval('dummyseq'::regclass);

-- Sequence relation exists, but it has no attributes.
SELECT * FROM dummyseq;

-- Reset connection, which will reset the sequence
\c
SELECT nextval('dummyseq'::regclass);

DROP SEQUENCE dummyseq;
DROP EXTENSION dummy_sequence_am;
