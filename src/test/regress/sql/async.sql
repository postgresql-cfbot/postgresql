--
-- ASYNC
--

--Should work. Send a valid message via a valid channel name
SELECT pg_notify('notify_async1','sample message1');
SELECT pg_notify('notify_async1','');
SELECT pg_notify('notify_async1',NULL);

-- Should fail. Send a valid message via an invalid channel name
SELECT pg_notify('','sample message1');
SELECT pg_notify(NULL,'sample message1');
SELECT pg_notify('notify_async_channel_name_too_long______________________________','sample_message1');

-- Should work. Valid NOTIFY commands, multiple levels
NOTIFY a;
NOTIFY a.b;
NOTIFY a.b.c;

-- Should fail. Invalid NOTIFY commands, empty levels
NOTIFY a.b.;
NOTIFY .b.c;
NOTIFY a..c;

-- Should work. Valid LISTEN/UNLISTEN commands, multiple levels and wildcards
LISTEN a;
LISTEN %;
LISTEN a%;
LISTEN >;
LISTEN a>;
LISTEN a.b;
LISTEN %.b;
LISTEN %.b%;
LISTEN a.>;
LISTEN a.b>;
LISTEN a.b.c;
LISTEN a.b%.a>;
UNLISTEN a;
UNLISTEN %;
UNLISTEN a%;
UNLISTEN >;
UNLISTEN a>;
UNLISTEN a.b;
UNLISTEN %.b;
UNLISTEN %.b%;
UNLISTEN a.>;
UNLISTEN a.b>;
UNLISTEN a.b.c;
UNLISTEN a.b%.a>;
UNLISTEN *;

-- Should fail. Invalid LISTEN/UNLISTEN commands, empty levels
LISTEN a.b%.;
LISTEN .b%.c>;
LISTEN a..%;
UNLISTEN a.b%.;
UNLISTEN .b%.c>;
UNLISTEN a..%;

-- Should fail. Invalid LISTEN/UNLISTEN commands, the wildcard '%' can only be
-- located at the end of a level
LISTEN %a;
LISTEN %>;
UNLISTEN %a;
UNLISTEN %>;

-- Should fail. Invalid LISTEN/UNLISTEN commands, the wildcard '>' can only be
-- located at the end of a channel name
LISTEN >.;
LISTEN >a;
LISTEN >%;
UNLISTEN >.;
UNLISTEN >a;
UNLISTEN >%;

-- Should return zero while there are no pending notifications.
-- src/test/isolation/specs/async-notify.spec tests for actual usage.
SELECT pg_notification_queue_usage();
