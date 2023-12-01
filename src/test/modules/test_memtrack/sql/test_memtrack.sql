
-- verify the pg_stat_memory_allocation view exists
SELECT
        pid > 0, total_reserved >= 0, init_reserved >= 0, aset_reserved >= 0, dsm_reserved >= 0, generation_reserved >= 0, slab_reserved >= 0
FROM
    pg_stat_memory_reservation limit 1;

-- verify the pg_stat_global_memory_tracking view exists
SELECT
        total_memory_reserved >= 0, dsm_memory_reserved >= 0, total_memory_available >= 0, static_shared_memory >= 0
FROM
    pg_stat_global_memory_tracking;

-- verify some common backends have reserved memory
SELECT
        total_reserved >= 0 AS result
FROM
    pg_stat_activity ps
        JOIN pg_stat_memory_reservation pa ON (pa.pid = ps.pid)
WHERE
        backend_type IN ('checkpointer', 'background writer', 'walwriter', 'autovacuum launcher');


-- For each process, the total should be the sum of subtotals
SELECT *
FROM
    pg_stat_memory_reservation
WHERE total_reserved != (init_reserved + aset_reserved + dsm_reserved + generation_reserved + slab_reserved);

-- For each process, the initial allocation is >= 1 MB
SELECT *
FROM
    pg_stat_memory_reservation
WHERE
    init_reserved < 1024*1024;

-- For current backend, the top context should match the sum from the allocators.
SELECT *
FROM
    pg_backend_memory_allocation
WHERE total_top_context_allocated <> (aset_allocated + generation_allocated + slab_allocated);

CREATE EXTENSION test_memtrack;

-- Make sure we can track memory usage of a single task
-- Since logic is the same for all memory managers, only test one.
SELECT test_memtrack(1, 1, 0, 1024);

-- Make sure we can track memory usage of multiple tasks.
--  By default we are limited to 8 tasks, so stay below the limit.
SELECT test_memtrack(5, 3, 1024, 5*1024);

-- Do it again. We had a bug where the second call would fail.
SELECT test_memtrack(5, 3, 1024, 5*1024);

-- Now we're going to actually do memory allocations.
-- We'll test each type of memory allocator.

-- Verify we can create and destroy contexts.
SELECT test_allocation(1,1,0,1024);
SELECT test_allocation(1,2,0, 1024);
SELECT test_allocation(1,3, 0,1024);
SELECT test_allocation(1,4,0,1024);

-- Create and free blocks of memory.
SELECT test_allocation(5,1,5*1024,1024);
SELECT test_allocation(5,2,5,1024*1024);  /* Fewer, don't exceed shmem limit */
SELECT test_allocation(5,3,5*1024,1024);
SELECT test_allocation(5,4,5*1024,1024);

-- Add up the private memory for each process and compare with the global total
--  The delta should be 0.
SELECT ABS(process_private - global_private) as delta
FROM
    (SELECT SUM(total_reserved - dsm_reserved)                             AS process_private
    FROM pg_stat_memory_reservation as p),
    (SELECT (total_memory_reserved - dsm_memory_reserved - static_shared_memory) AS global_private
    FROM pg_stat_global_memory_tracking as g);

-- Verify the global dsm memory is at least the sum of processes dsm memory.
-- The global can be larger if some process pinned dsm and than exited.
SELECT *
FROM
    (SELECT SUM(dsm_reserved) as process_dsm from pg_stat_memory_reservation),
    (SELECT dsm_memory_reserved as global_dsm from pg_stat_global_memory_tracking)
WHERE
    global_dsm < process_dsm;

-- Allocate more memory than we have available.
-- (this should fail because we configured max_total_memory to 1024 Mb)
SELECT test_memtrack(5, 2, 1024, 1024*1024*1024);
