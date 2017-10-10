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

-- Should work. Valid NOTIFY/LISTEN/UNLISTEN commands
-- src/test/isolation/specs/async-notify-2.spec tests for actual usage.
NOTIFY notify_async2;
LISTEN notify_async2;
LISTEN SIMILAR TO 'notify_%';
UNLISTEN notify_async2;
UNLISTEN 'notify_%';
UNLISTEN 'notify_(%';
UNLISTEN *;

-- Should fail. Invalid LISTEN command
LISTEN *;
LISTEN notify_%;
LISTEN SIMILAR TO 'notify_(%';
LISTEN SIMILAR TO '*';

-- Should contain two listeners
LISTEN notify_async2;
LISTEN SIMILAR TO 'notify_async2';
LISTEN SIMILAR TO 'notify_%';
SELECT pg_listening_channels();

-- Should contain one listener
UNLISTEN 'notify_%';
SELECT pg_listening_channels();

-- Should not contain listeners
UNLISTEN *;
SELECT pg_listening_channels();

-- Should return zero while there are no pending notifications.
-- src/test/isolation/specs/async-notify.spec tests for actual usage.
SELECT pg_notification_queue_usage();
