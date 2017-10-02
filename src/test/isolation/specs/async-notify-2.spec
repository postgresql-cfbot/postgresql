# Verify that messages are consumed from the notify queue.

session "listener" 
step "listen_normal"			{ LISTEN test; }
step "listen_pattern_1"			{ LISTEN SIMILAR TO 'te%'; }
step "listen_pattern_2"			{ LISTEN SIMILAR TO 'test'; }
step "listen_pattern_3"			{ LISTEN SIMILAR TO 'te'; }
step "listen_pattern_invalid"	{ LISTEN SIMILAR TO '*'; }
step "unlisten_1"				{ UNLISTEN 't%'; }
step "consume"					{ BEGIN; END; }
teardown						{ UNLISTEN *; }

session "notifier"
step "notify"	
{ 
 SELECT count(pg_notify('test', s::text)) FROM generate_series(1, 5) s;
 SELECT count(pg_notify('test_2', s::text)) FROM generate_series(1, 5) s;
}

# Should print first notify channel
permutation "listen_normal" "notify" "consume"
# Should print both notify channels
permutation "listen_pattern_1" "notify" "consume"
# Should print first notify channel
permutation "listen_pattern_2" "notify" "consume"
# Should not print either notify channels
permutation "listen_pattern_3" "notify" "consume"
# Should fail to invalid RE pattern
permutation "listen_pattern_invalid" "notify" "consume"
# Test that UNLISTEN with a pattern does not work as a RE matcher
permutation "listen_normal" "listen_pattern_1" "unlisten_1" "notify" "consume"