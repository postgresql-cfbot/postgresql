setup
{
    CREATE EXTENSION test_shm_mcxt;
}

teardown
{
    DROP EXTENSION test_shm_mcxt;
}

session "s1"
step "add_s1_1" {CALL set_shared_list(1);}
step "remove_s1_1" {CALL set_shared_list(-1);}
step "remove_s1_2" {CALL set_shared_list(-2);}
step "get_s1" {SELECT * from get_shared_list();}

session "s2"
step "add_s2_2" {CALL set_shared_list(2);}
step "remove_s2_1" {CALL set_shared_list(-1);}
step "get_s2" {SELECT * from get_shared_list();}

permutation "add_s1_1" "add_s2_2" "get_s1" "remove_s1_2" "remove_s2_1" "get_s1"
permutation "add_s1_1" "get_s2" "remove_s1_2" "get_s1"

"add_s1_1" "remove_s1_1" "remove_s2_1"
