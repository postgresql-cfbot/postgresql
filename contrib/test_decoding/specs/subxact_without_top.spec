# Forces decoding session to see (and associate with toplevel before commit)
# subxact with previous top records (first xl_xact_assignment) beyond
# restart_lsn. Such partially seen xact won't be streamed as it must finish
# before confirmed_flush_lsn, but nevertheless this harmless schedule used to
# cause
#   ERROR: subtransaction logged without previous top-level txn record
# failures.

setup
{
    SELECT 'init' FROM pg_create_logical_replication_slot('isolation_slot', 'test_decoding'); -- must be first write in xact
    CREATE TABLE harvest(apples integer);
    CREATE OR REPLACE FUNCTION subxacts() returns void as $$
    BEGIN
      FOR i in 1 .. 128 LOOP
        BEGIN
          INSERT INTO harvest VALUES (42);
        EXCEPTION
        WHEN OTHERS THEN
	  RAISE;
        END;
    END LOOP;
    END; $$LANGUAGE 'plpgsql';
}

teardown
{
    DROP TABLE IF EXISTS harvest;
    SELECT 'stop' FROM pg_drop_replication_slot('isolation_slot');
}

session "s0"
setup { SET synchronous_commit=on; }
step "s0_begin" { BEGIN; }
step "s0_first_subxact" {
    DO LANGUAGE plpgsql $$
      BEGIN
        BEGIN
          INSERT INTO harvest VALUES (41);
	EXCEPTION WHEN OTHERS THEN RAISE;
	END;
      END $$;
}
step "s0_many_subxacts" { select subxacts(); }
step "s0_commit" { COMMIT; }

session "s1"
setup { SET synchronous_commit=on; }
step "s1_begin" { BEGIN; }
step "s1_dml" { INSERT INTO harvest VALUES (43); }
step "s1_commit" { COMMIT; }

session "s2"
setup { SET synchronous_commit=on; }
step "s2_checkpoint" { CHECKPOINT; }
step "s2_get_changes" { SELECT data FROM pg_logical_slot_get_changes('isolation_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1'); }
step "s2_get_changes_suppress_output" { SELECT null n FROM pg_logical_slot_get_changes('isolation_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1') GROUP BY n; }

# First s2_checkpoint serializes snapshot (creates potential restart_lsn point)
# *after* initial subxact, i.e. first xl_xact_assignment is beyond it.
# s0_many_subxacts forces second xl_xact_assignment (issues >
# PGPROC_MAX_CACHED_SUBXIDS) to associate subxact with toplevel before commit.
# Second s2_checkpoint ensures s2_get_changes directly following it will
# advance the slot, establishing that restart_lsn.
# We do get_changes twice because if one more xl_running_xacts record had slipped
# before our CHECKPOINT, confirmed_flush will be advanced only to that record.
permutation "s0_begin" "s0_first_subxact" "s2_checkpoint" "s1_begin" "s1_dml" "s0_many_subxacts" "s0_commit" "s2_checkpoint" "s2_get_changes_suppress_output" "s2_get_changes_suppress_output" "s1_commit" "s2_get_changes"
