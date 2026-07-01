--
-- Test the matched-filter rule for FOR KEY joins through derived tables.
--
-- A derived table whose WHERE filters its rows is "row-preserving modulo
-- that filter".  A key join may use such a derived table on the
-- referenced (PK) side when the referencing (FK) side applies a matching
-- direct key equality filter on the corresponding key columns.  The value
-- side must still match structurally after constant folding and FK-mapped Var
-- substitution, and the filter must use the exact key identity for that FK
-- position.
--

CREATE SCHEMA mf;
SET search_path = mf, public;

-- ===========================================================================
-- Multi-tenant schema: composite FK, both sides parameterised by tenant_id.
-- ===========================================================================

CREATE TABLE customers (
    tenant_id INTEGER NOT NULL,
    id        INTEGER NOT NULL,
    name      TEXT    NOT NULL,
    active    BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE orders (
    tenant_id   INTEGER NOT NULL,
    id          INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    amount      NUMERIC NOT NULL,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, customer_id) REFERENCES customers (tenant_id, id)
);

INSERT INTO customers VALUES
    (1, 100, 'Alice',  true),
    (1, 101, 'Bob',    false),
    (2, 100, 'Carol',  true),
    (2, 200, 'Dave',   true);
INSERT INTO orders VALUES
    (1, 1, 100, 9.99),
    (1, 2, 101, 19.99),
    (2, 1, 100,  4.50),
    (2, 2, 200, 49.00);

CREATE VIEW tenant_customers AS
    SELECT * FROM customers
    WHERE tenant_id = current_setting('mf.tenant')::int;

CREATE VIEW tenant_orders AS
    SELECT * FROM orders
    WHERE tenant_id = current_setting('mf.tenant')::int;

SET mf.tenant = '1';

-- M1: Canonical multi-tenant case -- accepted
SELECT o.id, c.name
FROM tenant_orders o
JOIN tenant_customers c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY o.id;

-- M1b: Matched filters and FK columns may use non-first direct aliases -- accepted
CREATE VIEW customer_aliases AS
    SELECT tenant_id AS tenant_id_1, tenant_id AS tenant_id_2,
           id AS id_1, id AS id_2, name
    FROM customers;
CREATE VIEW order_aliases AS
    SELECT tenant_id AS tenant_id_1, tenant_id AS tenant_id_2,
           id, customer_id AS customer_id_1, customer_id AS customer_id_2,
           amount
    FROM orders;
CREATE VIEW tenant_customers_dup AS
    SELECT * FROM customer_aliases
    WHERE tenant_id_2 = current_setting('mf.tenant')::int;
CREATE VIEW tenant_orders_dup AS
    SELECT * FROM order_aliases
    WHERE tenant_id_2 = current_setting('mf.tenant')::int;

SELECT o.id, c.name
FROM tenant_orders_dup o
JOIN tenant_customers_dup c
  FOR KEY (tenant_id_2, id_2) <- o (tenant_id_2, customer_id_2)
ORDER BY o.id;

DROP VIEW tenant_orders_dup, tenant_customers_dup, order_aliases, customer_aliases;

-- M1c: Same-domain key filters normalize through the base equality type -- accepted
CREATE DOMAIN tenant_domain AS integer;

CREATE TABLE domain_customers (
    tenant_id tenant_domain NOT NULL,
    id        INTEGER       NOT NULL,
    name      TEXT          NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE domain_orders (
    tenant_id   tenant_domain NOT NULL,
    id          INTEGER       NOT NULL,
    customer_id INTEGER       NOT NULL,
    amount      NUMERIC       NOT NULL,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, customer_id)
        REFERENCES domain_customers (tenant_id, id)
);

INSERT INTO domain_customers VALUES
    (1, 100, 'Adele'),
    (1, 101, 'Bert'),
    (2, 100, 'Cleo');
INSERT INTO domain_orders VALUES
    (1, 1, 100, 8.25),
    (1, 2, 101, 6.50),
    (2, 1, 100, 4.75);

CREATE VIEW domain_tenant_customers AS
    SELECT * FROM domain_customers
    WHERE tenant_id = 1;

CREATE VIEW domain_tenant_orders AS
    SELECT * FROM domain_orders
    WHERE tenant_id = 1;

SELECT o.id, c.name
FROM domain_tenant_orders o
JOIN domain_tenant_customers c
  FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY o.id;

DROP VIEW domain_tenant_orders, domain_tenant_customers;
DROP TABLE domain_orders, domain_customers;
DROP DOMAIN tenant_domain;

-- Sanity: matches the explicit base-table form.
SELECT o.id, c.name
FROM orders o
JOIN customers c
  ON c.tenant_id = o.tenant_id AND c.id = o.customer_id
WHERE o.tenant_id = current_setting('mf.tenant')::int
  AND c.tenant_id = current_setting('mf.tenant')::int
ORDER BY o.id;

-- M2: FK side missing the filter -- rejected, reason: referenced filter is not matched on the referencing side
SELECT o.id, c.name
FROM orders o
JOIN tenant_customers c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

-- M2b: Join-local FILTER on the referencing key does not supply operand
-- matched-filter evidence -- rejected.
SELECT o.id, c.name
FROM orders o
JOIN tenant_customers c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
FILTER (WHERE o.tenant_id = current_setting('mf.tenant')::int);

-- M2c: Join-local FILTER on the referenced key also does not supply operand
-- matched-filter evidence -- rejected.
SELECT o.id, c.name
FROM orders o
JOIN tenant_customers c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
FILTER (WHERE c.tenant_id = current_setting('mf.tenant')::int);

-- M3: Different filter expressions (different constants) -- rejected, reason: referenced and referencing filters are not equivalent
CREATE VIEW tenant_customers_2 AS
    SELECT * FROM customers WHERE tenant_id = 2;
CREATE VIEW tenant_orders_1 AS
    SELECT * FROM orders WHERE tenant_id = 1;

SELECT o.id, c.name
FROM tenant_orders_1 o
-- rejected, reason: referenced and referencing filters use different constants
JOIN tenant_customers_2 c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW tenant_customers_2, tenant_orders_1;

-- M4: Composite filter on both sides matching FK composite columns -- accepted
CREATE VIEW c_pinpoint AS
    SELECT * FROM customers WHERE tenant_id = 1 AND id = 100;
CREATE VIEW o_pinpoint AS
    SELECT * FROM orders    WHERE tenant_id = 1 AND customer_id = 100;

SELECT o.id, c.name
FROM o_pinpoint o
-- accepted
JOIN c_pinpoint c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY o.id;

DROP VIEW c_pinpoint, o_pinpoint;

-- M5: Volatile filter -- rejected, reason: PK side filter contains random().
CREATE VIEW c_volatile AS
    SELECT * FROM customers WHERE tenant_id = (random()*100)::int;
CREATE VIEW o_volatile AS
    SELECT * FROM orders    WHERE tenant_id = (random()*100)::int;

SELECT o.id
FROM o_volatile o
-- rejected, reason: referenced-side filter contains volatile random()
JOIN c_volatile c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW c_volatile, o_volatile;

-- M6: Filter on a non-key column on the PK side -- rejected, reason: active is not part of the FK mapping
-- The PK conjunct on `active` references a column that isn't part of the
-- FK, so the conjunct cannot be carried across the FK mapping at all.
CREATE VIEW active_customers AS
    SELECT * FROM customers WHERE active;
CREATE VIEW any_orders AS
    SELECT * FROM orders WHERE amount > 0;

SELECT o.id, c.name
FROM any_orders o
-- rejected, reason: referenced-side filter is on a non-key column
JOIN active_customers c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW active_customers, any_orders;

-- M6b: OR on the referenced side -- rejected, reason: OR is not a safe
-- key-filter conjunct for referenced-side rowCoverage.
CREATE VIEW c_or_referenced AS
    SELECT * FROM customers
    WHERE tenant_id = current_setting('mf.tenant')::int OR active;

SELECT o.id, c.name
FROM tenant_orders o
-- rejected, reason: referenced-side OR filter drops rowCoverage
JOIN c_or_referenced c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW c_or_referenced;

-- M6c: OR on the referencing side -- rejected, reason: the referencing
-- FK fact must not retain tenant_id evidence from inside an OR.
CREATE VIEW o_or_referencing AS
    SELECT * FROM orders
    WHERE tenant_id = current_setting('mf.tenant')::int OR amount = 2;

SELECT o.id, c.name
FROM o_or_referencing o
-- rejected, reason: referencing-side OR filter does not match referenced filter
JOIN tenant_customers c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW o_or_referencing;

-- M6d: Matching-looking OR on both sides -- rejected, reason: OR is not a
-- safe matched-filter conjunct even when the same shape appears on both sides.
CREATE VIEW c_or_both AS
    SELECT * FROM customers
    WHERE tenant_id = current_setting('mf.tenant')::int OR id = 100;
CREATE VIEW o_or_both AS
    SELECT * FROM orders
    WHERE tenant_id = current_setting('mf.tenant')::int OR customer_id = 100;

SELECT o.id, c.name
FROM o_or_both o
-- rejected, reason: OR filters are not retained as matched-filter evidence
JOIN c_or_both c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW o_or_both, c_or_both;

-- M6e: A disjunction nested under a top-level AND is still not a
-- conjunct the proof system can retain, even when both sides have the
-- same mapped Boolean shape.
CREATE VIEW c_or_nested AS
    SELECT * FROM customers
    WHERE tenant_id = current_setting('mf.tenant')::int
      AND (id = 100 OR tenant_id = 2);
CREATE VIEW o_or_nested AS
    SELECT * FROM orders
    WHERE tenant_id = current_setting('mf.tenant')::int
      AND (customer_id = 100 OR tenant_id = 2);

SELECT o.id, c.name
FROM o_or_nested o
-- rejected, reason: nested Boolean filters are not retained as matched-filter evidence
JOIN c_or_nested c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW o_or_nested, c_or_nested;

-- M7: Two-layer view, filter in the inner layer -- accepted
CREATE VIEW c_inner AS
    SELECT * FROM customers
    WHERE tenant_id = current_setting('mf.tenant')::int;
CREATE VIEW c_outer AS
    SELECT * FROM c_inner;
CREATE VIEW o_inner AS
    SELECT * FROM orders
    WHERE tenant_id = current_setting('mf.tenant')::int;
CREATE VIEW o_outer AS
    SELECT * FROM o_inner;

SELECT o.id, c.name
FROM o_outer o
-- accepted
JOIN c_outer c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY o.id;

DROP VIEW c_outer, c_inner, o_outer, o_inner;

-- M8: CTE on each side -- accepted
WITH
    co AS (SELECT * FROM customers WHERE tenant_id = current_setting('mf.tenant')::int),
    oo AS (SELECT * FROM orders    WHERE tenant_id = current_setting('mf.tenant')::int)
SELECT o.id, c.name
FROM oo o
JOIN co c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY o.id;

-- M9: Matched filter on PK side, FK side has the matched filter PLUS an extra
-- conjunct that doesn't reference FK columns -- accepted (PK-driven matching).
CREATE VIEW big_orders AS
    SELECT * FROM orders
    WHERE tenant_id = current_setting('mf.tenant')::int
      AND amount > 5;

SELECT o.id, c.name
FROM big_orders o
-- accepted
JOIN tenant_customers c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY o.id;

DROP VIEW big_orders;

-- M10: Matched filter on PK side has an extra conjunct that the FK side does
-- not -- rejected, reason: referenced-side active conjunct is not matched (strict p_PK = p_match: every referenced-side conjunct
-- must match).
CREATE VIEW strict_customers AS
    SELECT * FROM customers
    WHERE tenant_id = current_setting('mf.tenant')::int
      AND active;

SELECT o.id, c.name
FROM tenant_orders o
-- rejected, reason: referenced side has an unmatched active filter
JOIN strict_customers c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW strict_customers;

-- M11: System column on referenced side -- rejected, reason: system-column filters are not matched key filters (would have
-- crashed before the varattno > 0 guard, since bms_is_member errors on
-- negative members).
CREATE VIEW c_sys AS
    SELECT * FROM customers WHERE ctid IS NOT NULL;
CREATE VIEW o_sys AS
    SELECT * FROM orders    WHERE ctid IS NOT NULL;

SELECT o.id
FROM o_sys o
-- rejected, reason: referenced-side filter uses a system column
JOIN c_sys c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW c_sys, o_sys;

-- M12: Parameterised filters are not retained as matched-filter evidence.
PREPARE mf_p AS
    SELECT o.id, c.name
    FROM (SELECT * FROM orders    WHERE tenant_id = $1) o
    JOIN (SELECT * FROM customers WHERE tenant_id = $1) c
-- rejected, reason: parameterized filters are not matched-filter evidence
        FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
    ORDER BY o.id;

-- M12b: Asymmetric parameterised filter — referenced side has $1, the
-- referencing side has a constant value → rejected for the same reason.
PREPARE mf_p_bad AS
    SELECT o.id, c.name
    FROM (SELECT * FROM orders    WHERE tenant_id = 1) o
    JOIN (SELECT * FROM customers WHERE tenant_id = $1) c
-- rejected, reason: referenced-side parameterized filter is not proof evidence
        FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

-- M12c: Proof placeholders must not collide with real SQL parameters.
-- The referenced-side $1 used to be remapped as if it were the tenant_id
-- key placeholder, making customer_id = tenant_id look like it matched
-- id = $1.
CREATE TABLE param_customers (
    tenant_id INTEGER NOT NULL,
    id        INTEGER NOT NULL,
    name      TEXT    NOT NULL,
    PRIMARY KEY (tenant_id, id)
);
CREATE TABLE param_orders (
    tenant_id   INTEGER NOT NULL,
    id          INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, customer_id) REFERENCES param_customers (tenant_id, id)
);
INSERT INTO param_customers VALUES (1, 1, 'One'), (1, 2, 'Two');
INSERT INTO param_orders VALUES (1, 1, 1);

PREPARE mf_p_collision(int, int) AS
    SELECT o.id, c.name
    FROM (SELECT * FROM param_orders WHERE customer_id = tenant_id) o
    JOIN (SELECT * FROM param_customers WHERE id = $1) c
-- rejected, reason: referenced-side SQL parameter is not proof evidence
        FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP TABLE param_orders, param_customers;

-- M13: current_user inside a STABLE-driven filter on a column that IS
-- in the FK -- accepted. Adds a tenant_role column to both sides that
-- gets its value from the user, with a tenant_id-shaped FK so the
-- matched filter is on a column the FK column mapping covers.
DROP VIEW tenant_orders, tenant_customers;
ALTER TABLE customers ADD COLUMN owner_role NAME NOT NULL DEFAULT current_user;
ALTER TABLE orders    ADD COLUMN owner_role NAME NOT NULL DEFAULT current_user;

-- Extend the existing FK so owner_role is one of the matched columns.
ALTER TABLE orders    DROP CONSTRAINT orders_tenant_id_customer_id_fkey;
ALTER TABLE customers DROP CONSTRAINT customers_pkey;
ALTER TABLE customers ADD CONSTRAINT customers_pkey
    PRIMARY KEY (tenant_id, owner_role, id);
ALTER TABLE orders    ADD FOREIGN KEY (tenant_id, owner_role, customer_id)
    REFERENCES customers (tenant_id, owner_role, id);

CREATE VIEW c_mine AS
    SELECT * FROM customers WHERE owner_role = current_user;
CREATE VIEW o_mine AS
    SELECT * FROM orders    WHERE owner_role = current_user;

SELECT o.id, c.name
FROM o_mine o
-- accepted
JOIN c_mine c FOR KEY (tenant_id, owner_role, id)
              <- o (tenant_id, owner_role, customer_id)
ORDER BY o.id, c.tenant_id;

DROP VIEW c_mine, o_mine;
-- Restore the original schema for subsequent cases.
ALTER TABLE orders    DROP CONSTRAINT orders_tenant_id_owner_role_customer_id_fkey;
ALTER TABLE customers DROP CONSTRAINT customers_pkey;
ALTER TABLE customers ADD CONSTRAINT customers_pkey PRIMARY KEY (tenant_id, id);
ALTER TABLE orders    ADD FOREIGN KEY (tenant_id, customer_id)
    REFERENCES customers (tenant_id, id);
ALTER TABLE customers DROP COLUMN owner_role;
ALTER TABLE orders    DROP COLUMN owner_role;
CREATE VIEW tenant_customers AS
    SELECT * FROM customers WHERE tenant_id = current_setting('mf.tenant')::int;
CREATE VIEW tenant_orders AS
    SELECT * FROM orders    WHERE tenant_id = current_setting('mf.tenant')::int;

-- M14: Inequality filter on both sides -- rejected, reason: matched filters
-- must be direct equality predicates.
-- Arbitrary inequalities are not accepted just because their expression
-- trees match.
CREATE VIEW c_ineq AS SELECT * FROM customers WHERE tenant_id < 10;
CREATE VIEW o_ineq AS SELECT * FROM orders    WHERE tenant_id < 10;

SELECT o.id, c.name
FROM o_ineq o
-- rejected, reason: matched filters must be direct equality predicates
JOIN c_ineq c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY o.id;

DROP VIEW c_ineq, o_ineq;

-- M15: Nested key join through conditionally row-preserved derived tables.
-- The inner key join (payments → orders) accepts under the
-- matched-filter rule.  The outer key join (orders → customers)
-- should also accept by propagating the matched row-coverage chain
-- from payments through orders to customers.
CREATE TABLE payments (
    tenant_id INTEGER NOT NULL,
    id        INTEGER NOT NULL,
    order_id  INTEGER NOT NULL,
    amount    NUMERIC NOT NULL,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, order_id) REFERENCES orders (tenant_id, id)
);

INSERT INTO payments VALUES
    (1, 9001, 1, 9.99),
    (1, 9002, 2, 19.99),
    (2, 9001, 1, 4.50);

CREATE VIEW tenant_payments AS
    SELECT * FROM payments
    WHERE tenant_id = current_setting('mf.tenant')::int;

SELECT o.id AS order_id, c.name AS customer_name, p.amount
FROM tenant_payments p
JOIN tenant_orders o
-- accepted
    FOR KEY (tenant_id, id) <- p (tenant_id, order_id)
JOIN tenant_customers c
-- accepted
    FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY p.id;

-- M15b: The middle orders input is unfiltered.  The first key join
-- proves p.tenant_id = o.tenant_id for rows in the result, so the
-- tenant_payments filter is materialized onto the output orders FK fact
-- and can satisfy tenant_customers in the second key join.
SELECT o.id AS order_id, c.name AS customer_name, p.amount
FROM tenant_payments p
JOIN orders o
-- accepted
    FOR KEY (tenant_id, id) <- p (tenant_id, order_id)
JOIN tenant_customers c
-- accepted
    FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY p.id;

DROP VIEW tenant_payments;
DROP TABLE payments;

-- M16: Multi-layer view, filter only at the OUTER layer -- accepted
-- The outer WHERE references the inner view's RTE; absorption translates
-- it through the inner view's targetlist (a SELECT * passthrough) into
-- the leaf base table's frame.
CREATE VIEW c_passthrough AS SELECT * FROM customers;
CREATE VIEW c_outer_filter AS
    SELECT * FROM c_passthrough WHERE tenant_id = 1;
CREATE VIEW o_t1 AS
    SELECT * FROM orders WHERE tenant_id = 1;

SELECT o.id, c.name
FROM o_t1 o
-- accepted
JOIN c_outer_filter c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY o.id, c.id;

DROP VIEW o_t1, c_outer_filter, c_passthrough;

-- M17: Multi-layer view with filters at BOTH layers -- accepted when
-- the referencing side applies the conjunction of both filters.
CREATE VIEW c_inner_tenant AS
    SELECT * FROM customers WHERE tenant_id = 1;
CREATE VIEW c_outer_id AS
    SELECT * FROM c_inner_tenant WHERE id = 100;
CREATE VIEW o_combined AS
    SELECT * FROM orders WHERE tenant_id = 1 AND customer_id = 100;

SELECT o.id, c.name
FROM o_combined o
-- accepted
JOIN c_outer_id c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW o_combined;

-- M18: Multi-layer view with filters at BOTH layers, but the referencing
-- side filters the FK column to a different value -- rejected, reason: referencing-side filter maps to a different key value.
-- Both the inner WHERE (tenant_id = 1) and outer WHERE (id = 100) must be
-- captured as matched-filter evidence; otherwise the join would accept while
-- silently dropping rows at runtime.
CREATE VIEW o_mismatch AS
    SELECT * FROM orders WHERE tenant_id = 1 AND customer_id = 999;

SELECT o.id, c.name
FROM o_mismatch o
-- rejected, reason: referencing side filters the FK column to a different value
JOIN c_outer_id c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW o_mismatch, c_outer_id, c_inner_tenant;

-- M19: Multi-layer view with LIMIT at the outer layer — hard disqualifier
-- at the absorbing layer, drops referenced-side rowCoverage entirely.
CREATE VIEW c_inner_for_lim AS
    SELECT * FROM customers WHERE tenant_id = 1;
CREATE VIEW c_outer_lim AS
    SELECT * FROM c_inner_for_lim LIMIT 5;

SELECT o.id, c.name
FROM tenant_orders o
-- rejected, reason: referenced-side outer view has LIMIT
JOIN c_outer_lim c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW c_outer_lim, c_inner_for_lim;

-- M20: Multi-layer view with a SubLink in the outer WHERE — the conjunct
-- cannot be represented as a safe key conjunct after Var translation, so the
-- referenced-side rowCoverage fact is dropped.
CREATE VIEW c_inner_for_sub AS
    SELECT * FROM customers WHERE tenant_id = 1;
CREATE VIEW c_outer_sub AS
    SELECT * FROM c_inner_for_sub WHERE EXISTS (SELECT 1);

SELECT o.id, c.name
FROM tenant_orders o
-- rejected, reason: referenced-side outer WHERE contains an unverifiable sublink
JOIN c_outer_sub c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW c_outer_sub, c_inner_for_sub;

-- M21: NextValueExpr (volatile, sequence-accessing) in a referenced-side
-- WHERE.  The capture-time contain_volatile_functions check flags this and
-- the entry is rejected as "filters this rule cannot verify".  Without that
-- check, two views carrying the structurally-identical nextval expression
-- would equal()-match in try_matched_filter and be unsoundly accepted, even
-- though their runtime evaluations would draw different sequence values.
CREATE SEQUENCE mf_seq;
CREATE VIEW c_nextval AS
    SELECT * FROM customers WHERE id = nextval('mf_seq')::int;
CREATE VIEW o_nextval AS
    SELECT * FROM orders    WHERE customer_id = nextval('mf_seq')::int;

SELECT o.id
FROM o_nextval o
-- rejected, reason: referenced-side filter contains volatile nextval()
JOIN c_nextval c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW c_nextval, o_nextval;
DROP SEQUENCE mf_seq;

-- M21b: User-defined same-name equality operators are not FK equality
-- evidence -- rejected, reason: matched-filter evidence must use the FK
-- position's common catalog equality operator.
CREATE FUNCTION mf_op_eq(integer, integer) RETURNS boolean
    LANGUAGE sql STABLE AS $$ SELECT $1 OPERATOR(pg_catalog.=) $2 $$;
CREATE OPERATOR = (
    LEFTARG = integer,
    RIGHTARG = integer,
    FUNCTION = mf_op_eq
);

CREATE VIEW op_customers AS
    SELECT * FROM customers WHERE tenant_id OPERATOR(mf.=) 1;
CREATE VIEW op_orders AS
    SELECT * FROM orders WHERE tenant_id OPERATOR(mf.=) 1;

SELECT o.id, c.name
FROM op_orders o
-- rejected, reason: mf.= is not the FK equality operator
JOIN op_customers c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

-- M21c: Propagated user-defined equality evidence is still rejected at the
-- consuming key join.
CREATE TABLE op_payments (
    tenant_id integer NOT NULL,
    id        integer NOT NULL,
    order_id  integer NOT NULL,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, order_id) REFERENCES orders (tenant_id, id)
);
INSERT INTO op_payments VALUES (1, 1, 1), (1, 2, 2), (2, 1, 1);
CREATE VIEW op_payments_v AS
    SELECT * FROM op_payments WHERE tenant_id OPERATOR(mf.=) 1;

SELECT p.id AS payment_id, o.id AS order_id, c.name
FROM op_payments_v p
JOIN orders o FOR KEY (tenant_id, id) <- p (tenant_id, order_id)
-- rejected, reason: propagated mf.= evidence is not FK equality evidence
JOIN op_customers c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW op_payments_v, op_orders, op_customers;
DROP TABLE op_payments;
DROP OPERATOR mf.= (integer, integer);
DROP FUNCTION mf_op_eq(integer, integer);

-- M22: View body with a non-key JOIN -- caught by the absence of exported
-- rowCoverage for arbitrary ordinary joins.  Only accepted key joins export
-- join-output facts to later key joins.
CREATE VIEW c_join AS
    SELECT a.tenant_id, a.id, a.name, a.active, b.amount
    FROM customers a
    JOIN orders b ON a.tenant_id = b.tenant_id AND a.id = b.customer_id
    WHERE a.tenant_id = 1;

SELECT o.id, c.name
FROM tenant_orders o
-- rejected, reason: referenced view body contains a non-key join
JOIN c_join c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW c_join;

-- M23: View with set-returning function in target list.  hasTargetSRFs is
-- caught by drill_down_to_base_rel_query at FK validation; this exercises
-- that the existing rejection still fires after the soundness changes.
CREATE VIEW c_srf AS
    SELECT generate_series(1, 5) AS gs, c.* FROM customers c WHERE tenant_id = 1;

SELECT o.id, c.name
FROM tenant_orders o
-- rejected, reason: referenced view target list contains a set-returning function
JOIN c_srf c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW c_srf;

-- M24: Direct equality predicate forms -- accepted.
CREATE VIEW c_types AS
    SELECT * FROM customers
    WHERE tenant_id = 1;
CREATE VIEW o_types AS
    SELECT * FROM orders
    WHERE tenant_id = 1;

SELECT o.id, c.name
FROM o_types o
-- accepted
JOIN c_types c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY o.id;

DROP VIEW o_types, c_types;

-- M24b: Richer predicate forms are no longer retained as proof evidence --
-- rejected, reason: matched-filter evidence is direct key equality only.
CREATE VIEW c_types AS
    SELECT * FROM customers
    WHERE tenant_id = 1
      AND tenant_id <> 99
      AND tenant_id IN (1, 2)
      AND tenant_id NOT IN (99, 100)
      AND tenant_id IS NOT NULL
      AND NOT (tenant_id = 99)
      AND tenant_id IS DISTINCT FROM 99
      AND (tenant_id = 1) IS TRUE;
CREATE VIEW o_types AS
    SELECT * FROM orders
    WHERE tenant_id = 1
      AND tenant_id <> 99
      AND tenant_id IN (1, 2)
      AND tenant_id NOT IN (99, 100)
      AND tenant_id IS NOT NULL
      AND NOT (tenant_id = 99)
      AND tenant_id IS DISTINCT FROM 99
      AND (tenant_id = 1) IS TRUE;

SELECT o.id, c.name
FROM o_types o
-- rejected, reason: richer predicates are not direct equality evidence
JOIN c_types c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY o.id;

DROP VIEW o_types, c_types;

-- M25: Key-column transformations -- rejected, reason: immutable functions can
-- distinguish values that the FK equality operator treats as equal, so a
-- structurally matching transformed-key predicate is not a safe matched filter.
CREATE VIEW c_transformed AS
    SELECT * FROM customers
    WHERE abs(tenant_id) = 1;
CREATE VIEW o_transformed AS
    SELECT * FROM orders
    WHERE abs(tenant_id) = 1;

SELECT o.id
FROM o_transformed o
-- rejected, reason: matched filter transforms the FK column
JOIN c_transformed c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW o_transformed, c_transformed;

-- M26: Numeric transformed-key counterexample -- rejected, reason: PostgreSQL
-- numeric equality treats 1.0 and 1.00 as equal, but scale(numeric) sees
-- different display scales; accepting this matched filter would drop the
-- preserved child row.
CREATE TABLE numeric_parent (
    id numeric PRIMARY KEY
);
CREATE TABLE numeric_child (
    id        integer PRIMARY KEY,
    parent_id numeric NOT NULL REFERENCES numeric_parent (id)
);

INSERT INTO numeric_parent VALUES (1.0);
INSERT INTO numeric_child VALUES (1, 1.00);

CREATE VIEW numeric_parent_scale2 AS
    SELECT * FROM numeric_parent WHERE scale(id) = 2;
CREATE VIEW numeric_child_scale2 AS
    SELECT * FROM numeric_child WHERE scale(parent_id) = 2;

SELECT c.id, p.id
FROM numeric_child_scale2 c
-- rejected, reason: scale() can distinguish values considered equal by FK equality
JOIN numeric_parent_scale2 p FOR KEY (id) <- c (parent_id);

SELECT id, parent_id FROM numeric_child_scale2;

CREATE VIEW numeric_parent_text AS
    SELECT * FROM numeric_parent WHERE id::text = '1.00';
CREATE VIEW numeric_child_text AS
    SELECT * FROM numeric_child WHERE parent_id::text = '1.00';

SELECT id, parent_id FROM numeric_child_text;

SELECT c.id, p.id
FROM numeric_child_text c
-- rejected, reason: text coercion can distinguish values considered equal by FK equality
JOIN numeric_parent_text p FOR KEY (id) <- c (parent_id);

DROP VIEW numeric_child_text, numeric_parent_text;
DROP VIEW numeric_child_scale2, numeric_parent_scale2;
DROP TABLE numeric_child, numeric_parent;

-- M26b: Numeric custom-operator counterexample -- rejected, reason: a
-- user-defined numeric equality filter can distinguish values that the FK
-- equality operator treats as equal.
CREATE FUNCTION numeric_text_eq(numeric, numeric) RETURNS boolean
    LANGUAGE sql IMMUTABLE AS $$ SELECT $1::text OPERATOR(pg_catalog.=) $2::text $$;
CREATE OPERATOR = (
    LEFTARG = numeric,
    RIGHTARG = numeric,
    FUNCTION = numeric_text_eq
);

CREATE TABLE numeric_op_parent (
    id numeric PRIMARY KEY
);
CREATE TABLE numeric_op_child (
    id        integer PRIMARY KEY,
    parent_id numeric NOT NULL REFERENCES numeric_op_parent (id)
);

INSERT INTO numeric_op_parent VALUES (1.0);
INSERT INTO numeric_op_child VALUES (1, 1.00);

CREATE VIEW numeric_op_parent_v AS
    SELECT * FROM numeric_op_parent WHERE id OPERATOR(mf.=) 1.00;
CREATE VIEW numeric_op_child_v AS
    SELECT * FROM numeric_op_child WHERE parent_id OPERATOR(mf.=) 1.00;

SELECT id, parent_id FROM numeric_op_child_v;
SELECT id FROM numeric_op_parent_v;

SELECT c.id, p.id
FROM numeric_op_child_v c
-- rejected, reason: mf.= is not the FK equality operator
JOIN numeric_op_parent_v p FOR KEY (id) <- c (parent_id);

DROP VIEW numeric_op_child_v, numeric_op_parent_v;
DROP TABLE numeric_op_child, numeric_op_parent;
DROP OPERATOR mf.= (numeric, numeric);
DROP FUNCTION numeric_text_eq(numeric, numeric);

-- M27: Currently-unsupported compound expression node types -- rejected, reason:
-- CollateExpr is not direct key equality.
-- This conjunct wraps the FK column tenant_id in an explicit COLLATE,
-- which produces a CollateExpr.  CollateExpr is not on the
-- direct-equality evidence path.  The default-reject policy correctly rejects
-- this conjunct as having a filter the rule cannot verify.
CREATE VIEW c_unsupported AS
    SELECT * FROM customers
    WHERE (tenant_id::text COLLATE "C") = '1';
CREATE VIEW o_unsupported AS
    SELECT * FROM orders
    WHERE (tenant_id::text COLLATE "C") = '1';

SELECT o.id
FROM o_unsupported o
-- rejected, reason: collated expression is not supported for matched filters
JOIN c_unsupported c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW o_unsupported, c_unsupported;

-- M28: Filter canonicalization must be RTE-aware -- rejected, reason:
-- the filter is on the child side's id column, not on the projected parent
-- key.  A varattno-only canonicalizer mistakes ch.id for p.id because both
-- are attno 1 in their own RTEs, and then falsely treats q.id as
-- row-covered under q.id = 10.
CREATE TABLE rte_filter_parent (
    id integer PRIMARY KEY
);
CREATE TABLE rte_filter_child (
    id        integer PRIMARY KEY,
    parent_id integer NOT NULL UNIQUE REFERENCES rte_filter_parent (id)
);
CREATE TABLE rte_filter_review (
    id        integer PRIMARY KEY,
    parent_id integer NOT NULL REFERENCES rte_filter_parent (id)
);

INSERT INTO rte_filter_parent VALUES (1), (10);
INSERT INTO rte_filter_child VALUES (10, 1);
INSERT INTO rte_filter_review VALUES (1, 10);

CREATE VIEW rte_filter_review_10 AS
    SELECT * FROM rte_filter_review WHERE parent_id = 10;

SELECT r.id, q.id
FROM (
    SELECT p.id
    FROM rte_filter_parent p
    LEFT JOIN rte_filter_child ch FOR KEY (parent_id) -> p (id)
    WHERE ch.id = 10
) q
-- rejected, reason: q is filtered by ch.id, not by q.id
JOIN rte_filter_review_10 r FOR KEY (parent_id) -> q (id);

DROP VIEW rte_filter_review_10;
DROP TABLE rte_filter_review, rte_filter_child, rte_filter_parent;

-- M29: Referencing-side filter evidence must not propagate to an
-- outer-preserved referenced side.  The LEFT/RIGHT subqueries still contain
-- parent id = 2, so they must not be treated as filtered to id = 1.
CREATE TABLE outer_filter_root (
    id integer PRIMARY KEY
);
CREATE TABLE outer_filter_parent (
    id integer PRIMARY KEY REFERENCES outer_filter_root (id)
);
CREATE TABLE outer_filter_child (
    id        integer PRIMARY KEY,
    parent_id integer NOT NULL REFERENCES outer_filter_parent (id)
);

INSERT INTO outer_filter_root VALUES (1), (2);
INSERT INTO outer_filter_parent VALUES (1), (2);
INSERT INTO outer_filter_child VALUES (10, 1);

CREATE VIEW outer_filter_root_1 AS
    SELECT * FROM outer_filter_root WHERE id = 1;
CREATE VIEW outer_filter_child_1 AS
    SELECT * FROM outer_filter_child WHERE parent_id = 1;

SELECT q.id
FROM (
    SELECT p.id
    FROM outer_filter_parent p
    LEFT JOIN outer_filter_child_1 c FOR KEY (parent_id) -> p (id)
) q
-- rejected, reason: LEFT JOIN preserves unmatched parent rows
JOIN outer_filter_root_1 r FOR KEY (id) <- q (id);

SELECT q.id
FROM (
    SELECT p.id
    FROM outer_filter_child_1 c
    RIGHT JOIN outer_filter_parent p FOR KEY (id) <- c (parent_id)
) q
-- rejected, reason: RIGHT JOIN preserves unmatched parent rows
JOIN outer_filter_root_1 r FOR KEY (id) <- q (id);

SELECT q.id
FROM (
    SELECT p.id
    FROM outer_filter_parent p
    JOIN outer_filter_child_1 c FOR KEY (parent_id) -> p (id)
) q
-- accepted, reason: INNER JOIN keeps only matched parent rows
JOIN outer_filter_root_1 r FOR KEY (id) <- q (id)
ORDER BY q.id;

DROP VIEW outer_filter_child_1, outer_filter_root_1;
DROP TABLE outer_filter_child, outer_filter_parent, outer_filter_root;

-- M30: Sublinks are not safe matched-filter conjuncts -- rejected, reason:
-- matching key = (SELECT ...) predicates must not be retained as
-- direct equality filter evidence.
CREATE VIEW c_sublink AS
    SELECT * FROM customers WHERE id = (SELECT 100);
CREATE VIEW o_sublink AS
    SELECT * FROM orders WHERE customer_id = (SELECT 100);

SELECT o.id, c.name
FROM o_sublink o
-- rejected, reason: matched filters contain sublinks
JOIN c_sublink c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW o_sublink, c_sublink;

-- M31: Cross-type FK key joins are rejected by key-join proof.  PostgreSQL
-- accepts the FK, but key join does not compute proof facts from it.
-- Supporting this would also require matched-filter proof to compare
-- filter evidence across different type-specific equality operators.
CREATE TABLE cross_type_customers (
    tenant_id smallint NOT NULL,
    id        integer  NOT NULL,
    name      text     NOT NULL,
    PRIMARY KEY (tenant_id, id)
);
CREATE TABLE cross_type_orders (
    tenant_id   bigint  NOT NULL,
    id          integer NOT NULL,
    customer_id integer NOT NULL,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, customer_id)
        REFERENCES cross_type_customers (tenant_id, id)
);

INSERT INTO cross_type_customers VALUES (1, 1, 'Alice'), (2, 1, 'Carol');
INSERT INTO cross_type_orders VALUES (1, 10, 1), (2, 20, 1);

SELECT o.id, c.name
FROM cross_type_orders o
-- rejected, reason: FK and PK key types differ
JOIN cross_type_customers c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY o.id;

CREATE VIEW cross_type_customers_1 AS
    SELECT * FROM cross_type_customers WHERE tenant_id = 1;
CREATE VIEW cross_type_orders_1 AS
    SELECT * FROM cross_type_orders WHERE tenant_id = 1;

SELECT o.id, c.name
FROM cross_type_orders_1 o
-- rejected, reason: FK and PK key types differ
JOIN cross_type_customers_1 c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);

DROP VIEW cross_type_orders_1, cross_type_customers_1;
DROP TABLE cross_type_orders, cross_type_customers;

-- Cleanup
RESET mf.tenant;
DROP VIEW tenant_orders, tenant_customers;
DROP TABLE orders, customers;
DROP SCHEMA mf;
RESET search_path;
