--
-- key_join_overview
--
-- One clear example per category, organized by SQL feature.
-- Shared fixture at top, all queries in the middle, all DROPs at bottom.
--

-- Referenced tables (PK side)
CREATE TABLE ref_products (
    prod_id   INTEGER PRIMARY KEY,
    prod_name TEXT NOT NULL
);

CREATE TABLE ref_locations (
    loc_country CHAR(2)     NOT NULL,
    loc_zip     TEXT        NOT NULL,
    PRIMARY KEY (loc_country, loc_zip)
);

CREATE TABLE ref_departments (
    dept_id   INTEGER PRIMARY KEY,
    dept_name TEXT NOT NULL
);

CREATE TABLE ref_teams (
    team_id      INTEGER PRIMARY KEY,
    team_dept_id INTEGER NOT NULL REFERENCES ref_departments(dept_id)
);

-- Referencing tables (FK side)
CREATE TABLE fk_orders (
    ord_id      INTEGER PRIMARY KEY,
    ord_prod_id INTEGER NOT NULL REFERENCES ref_products(prod_id)
);

CREATE TABLE fk_reviews (
    rev_id      INTEGER PRIMARY KEY,
    rev_prod_id INTEGER NOT NULL REFERENCES ref_products(prod_id)
);

CREATE TABLE fk_warehouses (
    wh_id      INTEGER PRIMARY KEY,
    wh_country CHAR(2)     NOT NULL,
    wh_zip     TEXT        NOT NULL,
    FOREIGN KEY (wh_country, wh_zip) REFERENCES ref_locations(loc_country, loc_zip)
);

CREATE TABLE fk_employees (
    emp_id      INTEGER PRIMARY KEY,
    emp_team_id INTEGER NOT NULL REFERENCES ref_teams(team_id)
);

-- Special-purpose tables
CREATE TABLE fk_returns (
    ret_id      INTEGER PRIMARY KEY,
    ret_prod_id INTEGER UNIQUE REFERENCES ref_products(prod_id)  -- nullable, unique
);

CREATE TABLE fk_inventory (
    inv_id      INTEGER PRIMARY KEY,
    inv_prod_id INTEGER NOT NULL UNIQUE REFERENCES ref_products(prod_id)
);

CREATE TABLE fk_deferrable_unique_inventory (
    dui_id      INTEGER PRIMARY KEY,
    dui_prod_id INTEGER NOT NULL REFERENCES ref_products(prod_id),
    CONSTRAINT fk_deferrable_unique_inventory_uniq
        UNIQUE (dui_prod_id) DEFERRABLE INITIALLY IMMEDIATE
);

-- For NOT VALID test
CREATE TABLE fk_notvalid_child (
    nv_id    INTEGER PRIMARY KEY,
    nv_p_id  INTEGER
);
ALTER TABLE fk_notvalid_child
    ADD CONSTRAINT fk_notvalid_child_fk
    FOREIGN KEY (nv_p_id) REFERENCES ref_products(prod_id) NOT VALID;

CREATE TABLE fk_notvalid_notnull_child (
    nvn_id   INTEGER PRIMARY KEY,
    nvn_p_id INTEGER REFERENCES ref_products(prod_id)
);

-- For NOT ENFORCED test
CREATE TABLE fk_notenforced_child (
    ne_id    INTEGER PRIMARY KEY,
    ne_p_id  INTEGER NOT NULL,
    FOREIGN KEY (ne_p_id) REFERENCES ref_products(prod_id) NOT ENFORCED
);

-- For deferred constraint-mode tests
CREATE TABLE fk_deferrable_child (
    df_id   INTEGER PRIMARY KEY,
    df_p_id INTEGER NOT NULL,
    CONSTRAINT fk_deferrable_child_fk
        FOREIGN KEY (df_p_id) REFERENCES ref_products(prod_id)
        DEFERRABLE INITIALLY IMMEDIATE
);

CREATE TABLE fk_initially_deferred_child (
    did_id   INTEGER PRIMARY KEY,
    did_p_id INTEGER NOT NULL,
    CONSTRAINT fk_initially_deferred_child_fk
        FOREIGN KEY (did_p_id) REFERENCES ref_products(prod_id)
        DEFERRABLE INITIALLY DEFERRED
);

-- For materialized view test
CREATE MATERIALIZED VIEW mv_products AS
SELECT prod_id, prod_name FROM ref_products;

-- For function tests
CREATE FUNCTION fn_products() RETURNS TABLE (prod_id INTEGER, prod_name TEXT)
LANGUAGE sql AS $$ SELECT prod_id, prod_name FROM ref_products $$;

SET check_function_bodies = off;
-- accepted, reason: function body is not checked while check_function_bodies is off
CREATE FUNCTION fn_deferrable_key_join()
RETURNS TABLE (prod_id INTEGER, df_id INTEGER)
LANGUAGE sql STABLE AS $$
    SELECT ref_products.prod_id, fk_deferrable_child.df_id
    FROM ref_products
    JOIN fk_deferrable_child FOR KEY (df_p_id) -> ref_products (prod_id)
$$;
RESET check_function_bodies;

-- For ghost-row clearing test
CREATE TABLE fko_clear_a (id INTEGER PRIMARY KEY);
CREATE TABLE fko_clear_c (id INTEGER PRIMARY KEY);
CREATE TABLE fko_clear_b (
    id   INTEGER PRIMARY KEY,
    a_id INTEGER NOT NULL REFERENCES fko_clear_a(id),
    c_id INTEGER NOT NULL REFERENCES fko_clear_c(id)
);
CREATE TABLE fko_clear_d (
    id   INTEGER PRIMARY KEY,
    b_id INTEGER NOT NULL REFERENCES fko_clear_b(id)
);

INSERT INTO ref_products VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Doohickey');
INSERT INTO ref_locations VALUES ('US', '10001'), ('DE', '80331'), ('JP', '10000');
INSERT INTO ref_departments VALUES (10, 'Engineering'), (20, 'Sales');
INSERT INTO ref_teams VALUES (100, 10), (200, 20);

INSERT INTO fk_orders VALUES (1, 1), (2, 1), (3, 2);
INSERT INTO fk_reviews VALUES (10, 1), (20, 3);
INSERT INTO fk_warehouses VALUES (1, 'US', '10001'), (2, 'DE', '80331');
INSERT INTO fk_employees VALUES (1000, 100), (2000, 200);

INSERT INTO fk_returns VALUES (1, 1), (2, NULL);
INSERT INTO fk_inventory VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO fk_deferrable_unique_inventory VALUES (1, 1), (2, 2), (3, 3);

INSERT INTO fk_notvalid_child VALUES (1, 1), (2, 2);
INSERT INTO fk_notvalid_notnull_child VALUES (1, 1), (2, NULL);
ALTER TABLE fk_notvalid_notnull_child
    ADD CONSTRAINT fk_notvalid_notnull_child_nn NOT NULL nvn_p_id NOT VALID;
INSERT INTO fk_notenforced_child VALUES (1, 1), (2, 2);
INSERT INTO fk_deferrable_child VALUES (1, 1), (2, 2);
INSERT INTO fk_initially_deferred_child VALUES (1, 1), (2, 2);

INSERT INTO fko_clear_a VALUES (1), (2), (3), (4);
INSERT INTO fko_clear_c VALUES (10), (20), (30);
INSERT INTO fko_clear_b VALUES (100, 1, 10), (200, 2, 20), (300, 3, 30);
INSERT INTO fko_clear_d VALUES (1000, 100), (2000, 200);

-- ============================================================
-- 1. Basic syntax and directions
-- ============================================================

SELECT ref_products.prod_id, fk_orders.ord_id
FROM ref_products
-- accepted
JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_id)
ORDER BY ord_id;

SELECT ref_products.prod_id, fk_orders.ord_id
FROM fk_orders
-- accepted
JOIN ref_products FOR KEY (prod_id) <- fk_orders (ord_prod_id)
ORDER BY ord_id;

-- rejected, reason: column count mismatch
SELECT * FROM ref_products
JOIN fk_orders FOR KEY (ord_prod_id, ord_id) -> ref_products (prod_id);

-- rejected, reason: column not found
SELECT * FROM ref_products
JOIN fk_orders FOR KEY (nonexistent) -> ref_products (prod_id);

-- rejected, reason: no matching FK constraint (prod_name is not the referenced column)
SELECT * FROM ref_products
JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_name);

-- rejected, reason: wrong direction (fk_orders has no FK referencing ref_products that way)
SELECT * FROM fk_orders
JOIN ref_products FOR KEY (prod_id) -> fk_orders (ord_prod_id);

-- ============================================================
-- 2. Join types (INNER, LEFT, RIGHT, FULL)
-- ============================================================

SELECT ref_products.prod_id, fk_orders.ord_id
FROM ref_products
-- accepted
JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_id)
ORDER BY ord_id;

SELECT ref_products.prod_id, fk_orders.ord_id
FROM ref_products
-- accepted
LEFT JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_id)
ORDER BY ref_products.prod_id, ord_id;

SELECT ref_products.prod_id, fk_orders.ord_id
FROM fk_orders
-- accepted
RIGHT JOIN ref_products FOR KEY (prod_id) <- fk_orders (ord_prod_id)
ORDER BY ref_products.prod_id, ord_id;

SELECT ref_products.prod_id, fk_orders.ord_id
FROM ref_products
-- accepted
FULL JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_id)
ORDER BY ref_products.prod_id, ord_id;

-- ============================================================
-- 3. Composite foreign keys
-- ============================================================

SELECT ref_locations.loc_country, fk_warehouses.wh_id
FROM ref_locations
-- accepted
JOIN fk_warehouses FOR KEY (wh_country, wh_zip) -> ref_locations (loc_country, loc_zip)
ORDER BY wh_id;

-- accepted, reason: swapped order on both sides
SELECT ref_locations.loc_country, fk_warehouses.wh_id
FROM ref_locations
JOIN fk_warehouses FOR KEY (wh_zip, wh_country) -> ref_locations (loc_zip, loc_country)
ORDER BY wh_id;

-- rejected, reason: mismatched column order
SELECT * FROM ref_locations
JOIN fk_warehouses FOR KEY (wh_zip, wh_country) -> ref_locations (loc_country, loc_zip);

-- ============================================================
-- 4. Derived tables (views, subqueries, CTEs)
-- ============================================================

-- accepted, reason: subquery wrapping base table as referenced side
SELECT q.prod_id, fk_orders.ord_id
FROM (SELECT * FROM ref_products) AS q
JOIN fk_orders FOR KEY (ord_prod_id) -> q (prod_id)
ORDER BY ord_id;

-- accepted, reason: subquery on referencing side
SELECT ref_products.prod_id, q.ord_id
FROM ref_products
JOIN (SELECT ord_id, ord_prod_id FROM fk_orders) AS q
    FOR KEY (ord_prod_id) -> ref_products (prod_id)
ORDER BY q.ord_id;

-- accepted, reason: nested subquery with column renames
SELECT q.p_id, q2.o_id
FROM (SELECT prod_id AS p_id FROM (SELECT prod_id FROM ref_products) sub) AS q
JOIN (SELECT ord_id AS o_id, ord_prod_id AS o_pid FROM fk_orders) AS q2
    FOR KEY (o_pid) -> q (p_id)
ORDER BY q2.o_id;

-- accepted, reason: view with column renames as referenced side
CREATE VIEW v_products_renamed AS
    SELECT prod_id AS pid, prod_name AS pname FROM ref_products;

SELECT v.pid, fk_orders.ord_id
FROM v_products_renamed v
-- accepted
JOIN fk_orders FOR KEY (ord_prod_id) -> v (pid)
ORDER BY ord_id;

-- accepted, reason: view containing FOR KEY, used as referenced side by outer FOR KEY
CREATE VIEW v_prods_with_inventory AS
    SELECT ref_products.prod_id, ref_products.prod_name, fk_inventory.inv_id
    FROM ref_products
    LEFT JOIN fk_inventory FOR KEY (inv_prod_id) -> ref_products (prod_id);

SELECT v.prod_id, fk_orders.ord_id
FROM v_prods_with_inventory v
-- accepted
JOIN fk_orders FOR KEY (ord_prod_id) -> v (prod_id)
ORDER BY ord_id;

-- accepted, reason: CTE chain with column renames
WITH
cte1 (cte_pid, cte_pname) AS (SELECT prod_id, prod_name FROM ref_products),
cte2 (c2_pid, c2_pname) AS (SELECT cte_pid, cte_pname FROM cte1)
SELECT c2_pid, fk_orders.ord_id
FROM cte2
JOIN fk_orders FOR KEY (ord_prod_id) -> cte2 (c2_pid)
ORDER BY ord_id;

-- rejected, reason: ordinary ON join in subquery does not expose key facts
SELECT q.prod_id, fk_orders.ord_id
FROM (
    SELECT ref_products.prod_id
    FROM ref_products
    LEFT JOIN fk_inventory ON fk_inventory.inv_prod_id = ref_products.prod_id
) q
JOIN fk_orders FOR KEY (ord_prod_id) -> q (prod_id);

-- rejected, reason: ordinary USING join in subquery does not expose key facts
SELECT q.prod_id, fk_orders.ord_id
FROM (
    SELECT p.prod_id
    FROM ref_products p
    LEFT JOIN (SELECT inv_prod_id AS prod_id FROM fk_inventory) i USING (prod_id)
) q
JOIN fk_orders FOR KEY (ord_prod_id) -> q (prod_id);

-- rejected, reason: ordinary CROSS JOIN with WHERE does not expose key facts
SELECT q.prod_id, fk_orders.ord_id
FROM (
    SELECT ref_products.prod_id
    FROM ref_products
    CROSS JOIN fk_inventory
    WHERE fk_inventory.inv_prod_id = ref_products.prod_id
) q
JOIN fk_orders FOR KEY (ord_prod_id) -> q (prod_id);

-- rejected, reason: comma join with WHERE does not expose key facts
SELECT q.prod_id, fk_orders.ord_id
FROM (
    SELECT ref_products.prod_id
    FROM ref_products, fk_inventory
    WHERE fk_inventory.inv_prod_id = ref_products.prod_id
) q
JOIN fk_orders FOR KEY (ord_prod_id) -> q (prod_id);

-- rejected, reason: view with WHERE on referenced side (R violated)
CREATE VIEW v_products_filtered AS
    SELECT prod_id, prod_name FROM ref_products WHERE prod_id > 0;

SELECT * FROM v_products_filtered v
-- accepted
JOIN fk_orders FOR KEY (ord_prod_id) -> v (prod_id);

-- rejected, reason: subquery with LIMIT on referenced side (R violated)
SELECT * FROM (SELECT prod_id FROM ref_products LIMIT 1) AS q
JOIN fk_orders FOR KEY (ord_prod_id) -> q (prod_id);

-- rejected, reason: TABLESAMPLE on referenced side (R violated)
SELECT * FROM ref_products rp TABLESAMPLE SYSTEM (0)
JOIN fk_orders FOR KEY (ord_prod_id) -> rp (prod_id);

-- rejected, reason: wrapped TABLESAMPLE still does not preserve row coverage
SELECT * FROM (SELECT prod_id FROM ref_products TABLESAMPLE SYSTEM (0)) AS q
JOIN fk_orders FOR KEY (ord_prod_id) -> q (prod_id);

-- rejected, reason: SKIP LOCKED on referenced side may remove rows at execution (R violated)
SELECT * FROM (SELECT prod_id FROM ref_products FOR UPDATE SKIP LOCKED) AS q
JOIN fk_orders FOR KEY (ord_prod_id) -> q (prod_id);

-- rejected, reason: recursive CTE
WITH RECURSIVE rcte AS (
    SELECT prod_id FROM ref_products
    UNION
    SELECT prod_id FROM rcte
)
SELECT * FROM rcte JOIN fk_orders FOR KEY (ord_prod_id) -> rcte (prod_id);

-- accepted, reason: two-level chain employees -> teams -> departments
SELECT ref_departments.dept_name, ref_teams.team_id, fk_employees.emp_id
FROM ref_departments
JOIN
    ref_teams JOIN fk_employees FOR KEY (emp_team_id) -> ref_teams (team_id)
FOR KEY (team_dept_id) -> ref_departments (dept_id)
ORDER BY emp_id;

-- ============================================================
-- 5. GROUP BY / DISTINCT / DISTINCT ON
-- ============================================================

-- accepted, reason: LEFT JOIN + GROUP BY on PK restores uniqueness
SELECT t3.rev_id, q.prod_id, q.cnt FROM
(
    SELECT ref_products.prod_id, COUNT(*) AS cnt
    FROM ref_products
    LEFT JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_id)
    GROUP BY ref_products.prod_id
) q
JOIN fk_reviews t3 FOR KEY (rev_prod_id) -> q (prod_id)
ORDER BY t3.rev_id;

-- accepted, reason: DISTINCT on full PK restores uniqueness
SELECT t3.rev_id, q.prod_id FROM
(
    SELECT DISTINCT ref_products.prod_id
    FROM ref_products
    LEFT JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_id)
) q
JOIN fk_reviews t3 FOR KEY (rev_prod_id) -> q (prod_id)
ORDER BY t3.rev_id;

-- accepted, reason: DISTINCT ON covering PK
SELECT t3.rev_id, q.prod_id FROM
(
    SELECT DISTINCT ON (ref_products.prod_id)
        ref_products.prod_id, fk_orders.ord_id
    FROM ref_products
    LEFT JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_id)
) q
JOIN fk_reviews t3 FOR KEY (rev_prod_id) -> q (prod_id)
ORDER BY t3.rev_id;

-- rejected, reason: GROUP BY on expression
SELECT * FROM
(
    SELECT ref_products.prod_id + 1 AS expr_result
    FROM ref_products
    LEFT JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_id)
    GROUP BY ref_products.prod_id + 1
) q
JOIN fk_reviews FOR KEY (rev_prod_id) -> q (expr_result);

-- rejected, reason: GROUP BY columns from different base tables
SELECT * FROM
(
    SELECT ref_products.prod_id, fk_orders.ord_id
    FROM ref_products
    LEFT JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_id)
    GROUP BY ref_products.prod_id, fk_orders.ord_id
) q
JOIN fk_reviews FOR KEY (rev_prod_id) -> q (prod_id);

-- rejected, reason: DISTINCT on partial composite PK
SELECT * FROM
(
    SELECT DISTINCT loc_country
    FROM ref_locations
) q
JOIN fk_warehouses FOR KEY (wh_country, wh_zip) -> q (loc_country, loc_zip);

-- rejected, reason: DISTINCT ON not covering PK
SELECT * FROM
(
    SELECT DISTINCT ON (fk_orders.ord_prod_id)
        ref_products.prod_id, fk_orders.ord_id
    FROM ref_products
    LEFT JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_id)
) q
JOIN fk_reviews FOR KEY (rev_prod_id) -> q (prod_id);

-- ============================================================
-- 6. Outer join null semantics
-- ============================================================

-- accepted, reason: referenced on preserved side of LEFT JOIN
SELECT COUNT(*)
FROM (
    SELECT ref_products.prod_id AS ref_id
    FROM ref_products
    LEFT JOIN fk_inventory FOR KEY (inv_prod_id) -> ref_products (prod_id)
) q
JOIN fk_orders FOR KEY (ord_prod_id) -> q (ref_id);

-- accepted, reason: FULL JOIN may null-extend ref_products, but ghost rows with NULL
-- ref_id never match the outer FK equi-join condition, so they neither
-- break uniqueness on q.ref_id nor prevent any fk_orders row from
-- finding its match.
SELECT COUNT(*)
FROM (
    SELECT ref_products.prod_id AS ref_id
    FROM ref_products
    FULL JOIN fk_returns FOR KEY (ret_prod_id) -> ref_products (prod_id)
) q
JOIN fk_orders FOR KEY (ord_prod_id) -> q (ref_id);

-- rejected, reason: FULL JOIN can null-extend the referencing side too, so
-- a NOT NULL fact on a referencing column must not be propagated through it.
-- fko_clear_a has id=4 with no matching fko_clear_b, so the FULL JOIN emits
-- a row with fko_clear_b.c_id IS NULL.  An INNER key join chained on c_id
-- would silently drop that null-extended row, violating preservation.
SELECT *
FROM fko_clear_a
FULL JOIN fko_clear_b FOR KEY (a_id) -> fko_clear_a (id)
JOIN fko_clear_c FOR KEY (id) <- fko_clear_b (c_id);

-- rejected, symmetric form: referencing on LEFT
SELECT *
FROM fko_clear_b
FULL JOIN fko_clear_a FOR KEY (id) <- fko_clear_b (a_id)
JOIN fko_clear_c FOR KEY (id) <- fko_clear_b (c_id);

-- accepted, sanity: INNER preserved-side JOIN does not null-extend, so the
-- chained INNER key join on c_id is still provable.
SELECT *
FROM fko_clear_a
JOIN fko_clear_b FOR KEY (a_id) -> fko_clear_a (id)
JOIN fko_clear_c FOR KEY (id) <- fko_clear_b (c_id)
ORDER BY fko_clear_b.id;

-- rejected, reason: non-unique key join inside subquery (U violated)
SELECT * FROM
(
    SELECT ref_products.prod_id, fk_orders.ord_id
    FROM ref_products
    JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_id)
) q
JOIN fk_reviews FOR KEY (rev_prod_id) -> q (prod_id);

-- rejected, reason: INNER JOIN inside subquery removes rows (R violated)
SELECT * FROM
(
    SELECT ref_products.prod_id
    FROM ref_products
    JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_id)
    GROUP BY ref_products.prod_id
) q
JOIN fk_reviews FOR KEY (rev_prod_id) -> q (prod_id);

-- ============================================================
-- 8. Constraint enforcement and timing
-- ============================================================

-- rejected, reason: NOT ENFORCED FK rejected
SELECT ref_products.prod_id, fk_notenforced_child.ne_id
FROM ref_products
JOIN fk_notenforced_child FOR KEY (ne_p_id) -> ref_products (prod_id)
ORDER BY ne_id;

-- rejected, reason: NOT VALID FK rejected
SELECT * FROM ref_products
JOIN fk_notvalid_child FOR KEY (nv_p_id) -> ref_products (prod_id);

-- rejected, reason: NOT VALID NOT NULL rejected
SELECT * FROM ref_products
JOIN fk_notvalid_notnull_child FOR KEY (nvn_p_id) -> ref_products (prod_id);

-- accepted, reason: NOT DEFERRABLE key join works even after SET CONSTRAINTS ALL DEFERRED
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
SELECT ref_products.prod_id, fk_orders.ord_id
FROM ref_products
-- accepted
JOIN fk_orders FOR KEY (ord_prod_id) -> ref_products (prod_id)
ORDER BY ord_id;
ROLLBACK;

-- rejected, reason: DEFERRABLE constraint rejected even when currently IMMEDIATE
SELECT ref_products.prod_id, fk_deferrable_child.df_id
FROM ref_products
JOIN fk_deferrable_child FOR KEY (df_p_id) -> ref_products (prod_id)
ORDER BY df_id;

-- rejected, reason: stored key join rejected when it depends on a DEFERRABLE constraint
CREATE VIEW v_deferrable_key_join AS
SELECT ref_products.prod_id, fk_deferrable_child.df_id
FROM ref_products
JOIN fk_deferrable_child FOR KEY (df_p_id) -> ref_products (prod_id);

-- rejected, reason: prepared key join rejected when it depends on a DEFERRABLE constraint
PREPARE p_deferrable_key_join AS
SELECT ref_products.prod_id, fk_deferrable_child.df_id
FROM ref_products
JOIN fk_deferrable_child FOR KEY (df_p_id) -> ref_products (prod_id)
ORDER BY df_id;

-- rejected, reason: cursor key join rejected when it depends on a DEFERRABLE constraint
BEGIN;
DECLARE c_deferrable_key_join CURSOR FOR
SELECT ref_products.prod_id, fk_deferrable_child.df_id
FROM ref_products
-- rejected, reason: cursor query depends on a DEFERRABLE FK
JOIN fk_deferrable_child FOR KEY (df_p_id) -> ref_products (prod_id)
ORDER BY df_id;
ROLLBACK;

-- rejected, reason: pulled-up subquery key join rejected at PREPARE
PREPARE p_pulled_up_deferrable_key_join AS
SELECT q.prod_id, q.df_id
FROM (
    SELECT ref_products.prod_id, fk_deferrable_child.df_id
    FROM ref_products
    JOIN fk_deferrable_child FOR KEY (df_p_id) -> ref_products (prod_id)
) q
ORDER BY df_id;

-- rejected, reason: inlined SQL function key join rejected when the body is planned
PREPARE p_inlined_deferrable_key_join AS
SELECT * FROM fn_deferrable_key_join()
ORDER BY df_id;

EXECUTE p_inlined_deferrable_key_join;

DEALLOCATE p_inlined_deferrable_key_join;

-- rejected, reason: DEFERRABLE constraint rejected when currently DEFERRED
BEGIN;
SET CONSTRAINTS fk_deferrable_child_fk DEFERRED;
SELECT * FROM ref_products
-- rejected, reason: DEFERRABLE FK is currently deferred
JOIN fk_deferrable_child FOR KEY (df_p_id) -> ref_products (prod_id);
ROLLBACK;

-- rejected, reason: INITIALLY DEFERRED constraint rejected by default
BEGIN;
SELECT * FROM ref_products
-- rejected, reason: FK is INITIALLY DEFERRED
JOIN fk_initially_deferred_child FOR KEY (did_p_id) -> ref_products (prod_id);
ROLLBACK;

-- rejected, reason: INITIALLY DEFERRED constraint rejected even after SET CONSTRAINTS IMMEDIATE
BEGIN;
SET CONSTRAINTS fk_initially_deferred_child_fk IMMEDIATE;
SELECT ref_products.prod_id, fk_initially_deferred_child.did_id
FROM ref_products
-- rejected, reason: INITIALLY DEFERRED FK remains unusable for key joins
JOIN fk_initially_deferred_child FOR KEY (did_p_id) -> ref_products (prod_id)
ORDER BY did_id;
ROLLBACK;

-- rejected, reason: DEFERRABLE UNIQUE does not preserve referenced-side uniqueness
SELECT q.prod_id, fk_reviews.rev_id
FROM (
    SELECT ref_products.prod_id
    FROM ref_products
    LEFT JOIN fk_deferrable_unique_inventory
        FOR KEY (dui_prod_id) -> ref_products (prod_id)
) q
JOIN fk_reviews FOR KEY (rev_prod_id) -> q (prod_id);

-- ============================================================
-- 9. Unsupported relation kinds
-- ============================================================

-- rejected, reason: function as FK source
SELECT * FROM ref_products
JOIN fn_products() AS f FOR KEY (prod_id) -> ref_products (prod_id);

-- rejected, reason: materialized view
SELECT * FROM mv_products
JOIN fk_orders FOR KEY (ord_prod_id) -> mv_products (prod_id);

-- rejected, reason: UNION ALL in derived table
SELECT * FROM ref_products
JOIN (
    SELECT ord_id, ord_prod_id FROM fk_orders
    UNION ALL
    SELECT ord_id, ord_prod_id FROM fk_orders
) AS u FOR KEY (ord_prod_id) -> ref_products (prod_id);

-- ============================================================
-- 10. Deep multi-feature stress test
-- ============================================================

-- accepted, reason: deeply-nested query stress-testing key join resolution
--
-- The key join analysis must drill through 6 levels to reach the base table:
--   Level 1: ref_products (base table with PK prod_id)
--   Level 2: LEFT JOIN with fk_orders (subquery on referencing side, column renamed)
--            + GROUP BY on base-table PK restoring uniqueness
--   Level 3: subquery wrapper with column rename
--   Level 4: subquery wrapper with column rename
--   Level 5: subquery wrapper with column rename
--   Level 6: outer key join with fk_reviews through all layers
--
SELECT rev.rev_id, deep.p_id, deep.order_count
FROM (
    -- Level 5: rename layer 3
    SELECT l4_id AS p_id, l4_cnt AS order_count
    FROM (
        -- Level 4: rename layer 2
        SELECT l3_id AS l4_id, l3_cnt AS l4_cnt
        FROM (
            -- Level 3: rename layer 1
            SELECT grp_id AS l3_id, grp_cnt AS l3_cnt
            FROM (
                -- Level 2: GROUP BY on base-table PK restoring uniqueness
                SELECT ref_products.prod_id AS grp_id, COUNT(*) AS grp_cnt
                FROM ref_products                  -- Level 1: base table
                LEFT JOIN (
                    -- subquery on referencing side with column rename
                    SELECT ord_prod_id AS opid FROM fk_orders
                ) ord_q FOR KEY (opid) -> ref_products (prod_id)
                GROUP BY ref_products.prod_id
            ) grp
        ) l3
    ) l4
) deep
-- Level 6: final key join reaching through all rename layers
JOIN fk_reviews rev FOR KEY (rev_prod_id) -> deep (p_id)
ORDER BY rev.rev_id;

-- accepted, reason: view-with-FK-join resolution chain + nested FK chain with <- direction
-- FROM: deep -> l2 -> l1 -> v_prods_with_inventory (view with key join inside)
-- SELECT: scalar subquery with 3-level chain using <- at inner level
SELECT rev.rev_id, deep.p_id,
       (SELECT COUNT(*)
        FROM ref_departments
        JOIN (
            fk_employees
            JOIN ref_teams FOR KEY (team_id) <- fk_employees (emp_team_id)
        ) FOR KEY (team_dept_id) -> ref_departments (dept_id)
       ) AS emp_count
FROM (
    SELECT l2_id AS p_id FROM (
        SELECT l1_id AS l2_id FROM (
            SELECT prod_id AS l1_id FROM v_prods_with_inventory
        ) l1
    ) l2
) deep
JOIN fk_reviews rev FOR KEY (rev_prod_id) -> deep (p_id)
ORDER BY rev.rev_id;

-- rejected, reason: row-filtering WHERE buried under rename layers (R violated)
SELECT rev.rev_id, deep.p_id
FROM (
    SELECT l3_id AS p_id FROM (
        SELECT l2_id AS l3_id FROM (
            SELECT l1_id AS l2_id FROM (
                SELECT prod_id AS l1_id
                FROM ref_products
                WHERE prod_name <> 'Widget'
            ) l1
        ) l2
    ) l3
) deep
JOIN fk_reviews rev FOR KEY (rev_prod_id) -> deep (p_id)
ORDER BY rev.rev_id;

-- ============================================================
-- Cleanup
-- ============================================================

DROP VIEW v_prods_with_inventory;
DROP VIEW v_products_renamed;
DROP VIEW v_products_filtered;

DROP FUNCTION fn_products();
DROP FUNCTION fn_deferrable_key_join();
DROP MATERIALIZED VIEW mv_products;

DROP TABLE fko_clear_d;
DROP TABLE fko_clear_b;
DROP TABLE fko_clear_c;
DROP TABLE fko_clear_a;

DROP TABLE fk_initially_deferred_child;
DROP TABLE fk_deferrable_child;
DROP TABLE fk_notenforced_child;
DROP TABLE fk_notvalid_notnull_child;
DROP TABLE fk_notvalid_child;
DROP TABLE fk_deferrable_unique_inventory;
DROP TABLE fk_inventory;
DROP TABLE fk_returns;
DROP TABLE fk_employees;
DROP TABLE fk_warehouses;
DROP TABLE fk_reviews;
DROP TABLE fk_orders;
DROP TABLE ref_teams;
DROP TABLE ref_departments;
DROP TABLE ref_locations;
DROP TABLE ref_products;
