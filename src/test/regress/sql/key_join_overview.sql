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

CREATE VIEW v_products_renamed AS
    SELECT prod_id AS pid, prod_name AS pname FROM ref_products;

CREATE VIEW v_products_filtered AS
    SELECT prod_id, prod_name FROM ref_products WHERE prod_id > 0;

-- rejected, reason: TABLESAMPLE on referenced side (R violated)
SELECT * FROM ref_products rp TABLESAMPLE SYSTEM (0)
JOIN fk_orders FOR KEY (ord_prod_id) -> rp (prod_id);

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

-- ============================================================
-- 9. Unsupported relation kinds
-- ============================================================

-- rejected, reason: materialized view
SELECT * FROM mv_products
JOIN fk_orders FOR KEY (ord_prod_id) -> mv_products (prod_id);

-- ============================================================
-- Cleanup
-- ============================================================

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
