-- tests for functions related to TID handling

CREATE TABLE tid_tab (a int);

-- min() and max() for TIDs
INSERT INTO tid_tab VALUES (1), (2);
SELECT min(ctid) FROM tid_tab;
SELECT max(ctid) FROM tid_tab;
TRUNCATE tid_tab;

DROP TABLE tid_tab CASCADE;
