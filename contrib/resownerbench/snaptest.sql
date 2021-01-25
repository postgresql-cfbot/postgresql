--
-- Performance test RegisterSnapshot/UnregisterSnapshot.
--
select numkeep, numsnaps,
       -- numiters,
       -- round(lifo_time_ms) as lifo_total_time_ms,
       -- round(fifo_time_ms) as fifo_total_time_ms,
       round((lifo_time_ms::numeric / (numkeep + (numsnaps - numkeep) * numiters)) * 1000000, 1) as lifo_time_ns,
       round((fifo_time_ms::numeric / (numkeep + (numsnaps - numkeep) * numiters)) * 1000000, 1) as fifo_time_ns
from
(values (0,      1,  10000000 * 10),
        (0,      5,   2000000 * 10),
        (0,     10,   1000000 * 10),
        (0,     60,    100000 * 10),
        (0,     70,    100000 * 10),
        (0,    100,    100000 * 10),
        (0,   1000,     10000 * 10),
        (0,  10000,      1000 * 10),

-- These tests keep 9 snapshots registered across the iterations. That
-- exceeds the size of the little array in the patch, so this exercises
-- the hash lookups. Without the patch, these still fit in the array
-- (it's 64 entries without the patch)
        (9,     10,  10000000 * 10),
        (9,    100,    100000 * 10),
        (9,   1000,     10000 * 10),
        (9,  10000,      1000 * 10),

-- These exceed the 64 entry array even without the patch, so these fall
-- in the hash table regime with and without the patch.
        (65,    70,   1000000 * 10),
        (65,   100,    100000 * 10),
        (65,  1000,     10000 * 10),
        (65, 10000,      1000 * 10)
) AS params (numkeep, numsnaps, numiters),
lateral snapshotbench_lifo(numkeep, numsnaps, numiters) as lifo_time_ms,
lateral snapshotbench_fifo(numkeep, numsnaps, numiters) as fifo_time_ms;
