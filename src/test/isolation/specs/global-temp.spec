# Test global temporary relations

setup {
  CREATE GLOBAL TEMP TABLE tmp (key int PRIMARY KEY, val text, seq serial);

  CREATE GLOBAL TEMP TABLE tmp_parted (key int PRIMARY KEY, val text) PARTITION BY LIST (key);
  CREATE GLOBAL TEMP TABLE tmp_p1 PARTITION OF tmp_parted FOR VALUES IN (1);
  CREATE GLOBAL TEMP TABLE tmp_p2 PARTITION OF tmp_parted FOR VALUES IN ((2), (3));
}

teardown {
  DROP TABLE tmp, tmp_parted;
}

session s1
setup { SET allow_in_place_tablespaces = true; }
step b1 { BEGIN; }
step create_tblspace { CREATE TABLESPACE regress_isolation_tablespace LOCATION ''; }
step list_tblspaces { SELECT spcname FROM pg_tablespace ORDER BY 1; }
step drop_tblspace { DROP TABLESPACE regress_isolation_tablespace; }
step ins1 { INSERT INTO tmp VALUES (1, 's1'); }
step ins1p1 { INSERT INTO tmp_parted VALUES (1, 's1 p1'); }
step ins1p2 { INSERT INTO tmp_parted VALUES (2, 's1 p2'); }
step sel1 { SELECT * FROM tmp; }
step sel1p { SELECT tableoid::regclass, * FROM tmp_parted; }
step create1 { CREATE GLOBAL TEMP TABLE tmp2 (key int, val text); }
step create1dr { CREATE GLOBAL TEMP TABLE tmp2 (key int, val text) ON COMMIT DELETE ROWS; }
step ins1_2 { INSERT INTO tmp2 VALUES (1, 's1'); }
step sel1_2 { SELECT * FROM tmp2; }
step alter1a { ALTER TABLE tmp2 ALTER COLUMN key SET DATA TYPE numeric; }
step alter1b { ALTER TABLE tmp2 ALTER COLUMN val SET NOT NULL; }
step seltype1 { SELECT key, pg_typeof(key), val FROM tmp2; }
step drop1 { DROP TABLE tmp2; }
step c1 { COMMIT; }
step idx1 { CREATE INDEX tmp_val_idx ON tmp(val); }
step sel1_idx {
  SET enable_seqscan = off;
  SET enable_bitmapscan = off;
  EXPLAIN (COSTS OFF)
  SELECT * FROM tmp WHERE val = 's1';
  SELECT * FROM tmp WHERE val = 's1';
}
step t1 { TRUNCATE tmp; }
step alt_tblspace { ALTER TABLE tmp SET TABLESPACE regress_isolation_tablespace; }
step get_tblspace1 {
  SELECT s1.spcname, s2.spcname,
         regexp_replace(pg_relation_filepath('tmp'), '(\d+)', 'NNN', 'g')
    FROM pg_class c
    JOIN pg_tablespace s1 ON s1.oid = c.reltablespace
    LEFT JOIN pg_temp_class t ON t.oid = c.oid
    JOIN pg_tablespace s2 ON s2.oid = t.reltablespace
   WHERE c.relname = 'tmp';
}
step reset_tblspace { ALTER TABLE tmp SET TABLESPACE pg_default; }

session s2
step b2 { BEGIN; }
step ins2 { INSERT INTO tmp VALUES (1, 's2'); }
step ins2p1 { INSERT INTO tmp_parted VALUES (1, 's2 p1'); }
step ins2p2 { INSERT INTO tmp_parted VALUES (2, 's2 p2'); }
step sel2 { SELECT * FROM tmp; }
step sel2p { SELECT tableoid::regclass, * FROM tmp_parted; }
step t2 { TRUNCATE tmp; }
step c2 { COMMIT; }
step r2 { ROLLBACK; }
step sp2 { SAVEPOINT sp; }
step rsp2 { ROLLBACK TO SAVEPOINT sp; }
step ins2_2 { INSERT INTO tmp2 VALUES (1, 's2'); }
step sel2_2 { SELECT * FROM tmp2; }
step seltype2 { SELECT key, pg_typeof(key), val FROM tmp2; }
step sel2_idx {
  SET enable_seqscan = off;
  SET enable_bitmapscan = off;
  EXPLAIN (COSTS OFF)
  SELECT * FROM tmp WHERE val = 's2';
  SELECT * FROM tmp WHERE val = 's2';
}
step reidx2 { REINDEX INDEX tmp_val_idx; }
step get_tblspace2 {
  SELECT s1.spcname, s2.spcname,
         regexp_replace(pg_relation_filepath('tmp'), '(\d+)', 'NNN', 'g')
    FROM pg_class c
    JOIN pg_tablespace s1 ON s1.oid = c.reltablespace
    LEFT JOIN pg_temp_class t ON t.oid = c.oid
    LEFT JOIN pg_tablespace s2 ON s2.oid = t.reltablespace
   WHERE c.relname = 'tmp';
}
step analyze2 { ANALYZE tmp; }

# Create test tablespace for remaining tests
permutation create_tblspace list_tblspaces

# Basic effects
permutation ins1 ins2 sel1 sel2
permutation ins1p1 ins1p2 ins2p1 ins2p2 sel1p sel2p

# Test rollback of GTT initialization
permutation ins1 b2 ins2 sel1 sel2 c2 sel1 sel2
permutation ins1 b2 ins2 sel1 sel2 r2 sel1 sel2
permutation ins1 b2 ins2 sel1 sel2 sp2 r2 sel1 sel2
permutation ins1 b2 sp2 ins2 sel1 sel2 rsp2 sel1 sel2 r2 sel1 sel2
permutation ins1 b2 ins2 sp2 t2 rsp2 sel1 sel2 r2 sel1 sel2

# Test prevention of ALTER TABLE with rewrite, if in use
permutation create1 ins1_2 alter1a alter1b ins2_2 seltype1 seltype2 drop1
permutation create1 ins1_2 ins2_2 alter1a alter1b seltype1 seltype2 drop1

# Test concurrent ON COMMIT DELETE ROWS
permutation create1dr b1 b2 ins1_2 ins2_2 sel1_2 sel2_2 c1 c2 sel1_2 sel2_2 drop1

# Test DROP with ON COMMIT DELETE ROWS
permutation create1dr ins1_2 ins2_2 drop1 create1dr ins1_2 ins2_2 drop1

# Test val index
permutation ins1 idx1 sel1_idx ins2 sel2_idx
permutation ins1 ins2 idx1 sel1_idx sel2_idx
permutation ins1 ins2 idx1 sel1_idx sel2_idx reidx2 sel2_idx
permutation ins1 ins2 idx1 sel1_idx sel2_idx analyze2 sel2_idx reidx2 sel2_idx

# Test local TRUNCATE
permutation ins1 ins2 t2 sel1 sel2 ins2 t1 sel1 sel2 ins1 t2 sel1 sel2

# Test ALTER TABLE ... SET TABLESPACE
permutation ins1 ins2 alt_tblspace get_tblspace1 get_tblspace2 sel1 sel2 reset_tblspace

# Tidy up
permutation drop_tblspace list_tblspaces
