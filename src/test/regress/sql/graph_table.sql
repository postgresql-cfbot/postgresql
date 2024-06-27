CREATE SCHEMA graph_table_tests;
GRANT USAGE ON SCHEMA graph_table_tests TO PUBLIC;
SET search_path = graph_table_tests;

CREATE TABLE products (
    product_no integer PRIMARY KEY,
    name varchar,
    price numeric
);

CREATE TABLE customers (
    customer_id integer PRIMARY KEY,
    name varchar,
    address varchar
);

CREATE TABLE orders (
    order_id integer PRIMARY KEY,
    ordered_when date
);

CREATE TABLE order_items (
    order_items_id integer PRIMARY KEY,
    order_id integer REFERENCES orders (order_id),
    product_no integer REFERENCES products (product_no),
    quantity integer
);

CREATE TABLE customer_orders (
    customer_orders_id integer PRIMARY KEY,
    customer_id integer REFERENCES customers (customer_id),
    order_id integer REFERENCES orders (order_id)
);

CREATE TABLE wishlists (
    wishlist_id integer PRIMARY KEY,
    wishlist_name varchar
);

CREATE TABLE wishlist_items (
    wishlist_items_id integer PRIMARY KEY,
    wishlist_id integer REFERENCES wishlists (wishlist_id),
    product_no integer REFERENCES products (product_no)
);

CREATE TABLE customer_wishlists (
    customer_wishlist_id integer PRIMARY KEY,
    customer_id integer REFERENCES customers (customer_id),
    wishlist_id integer REFERENCES wishlists (wishlist_id)
);

CREATE PROPERTY GRAPH myshop
    VERTEX TABLES (
        products,
        customers,
        orders
           DEFAULT LABEL
            LABEL lists PROPERTIES (order_id as node_id, 'order'::varchar(10) as list_type),
        wishlists
           DEFAULT LABEL
            LABEL lists PROPERTIES (wishlist_id as node_id, 'wishlist'::varchar(10) as list_type)
    )
    EDGE TABLES (
        order_items KEY (order_items_id)
            SOURCE KEY (order_id) REFERENCES orders (order_id)
            DESTINATION KEY (product_no) REFERENCES products (product_no)
            DEFAULT LABEL
            LABEL list_items PROPERTIES (order_id as link_id, product_no),
        wishlist_items KEY (wishlist_items_id)
            SOURCE KEY (wishlist_id) REFERENCES wishlists (wishlist_id)
            DESTINATION KEY (product_no) REFERENCES products (product_no)
            DEFAULT LABEL
            LABEL list_items PROPERTIES (wishlist_id as link_id, product_no),
        customer_orders KEY (customer_orders_id)
            SOURCE KEY (customer_id) REFERENCES customers (customer_id)
            DESTINATION KEY (order_id) REFERENCES orders (order_id)
            DEFAULT LABEL
            LABEL cust_lists PROPERTIES (customer_id, order_id as link_id),
        customer_wishlists KEY (customer_wishlist_id)
            SOURCE KEY (customer_id) REFERENCES customers (customer_id)
            DESTINATION KEY (wishlist_id) REFERENCES wishlists (wishlist_id)
            DEFAULT LABEL
            LABEL cust_lists PROPERTIES (customer_id, wishlist_id as link_id)
    );

SELECT customer_name FROM GRAPH_TABLE (xxx MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (pg_class MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (cx.name AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.namex AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers|employees WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders] COLUMNS (c.name AS customer_name));  -- error

INSERT INTO products VALUES
    (1, 'product1', 10),
    (2, 'product2', 20),
    (3, 'product3', 30);
INSERT INTO customers VALUES
    (1, 'customer1', 'US'),
    (2, 'customer2', 'CA'),
    (3, 'customer3', 'GL');
INSERT INTO orders VALUES
    (1, date '2024-01-01'),
    (2, date '2024-01-02'),
    (3, date '2024-01-03');
INSERT INTO wishlists VALUES
    (1, 'wishlist1'),
    (2, 'wishlist2'),
    (3, 'wishlist3');
INSERT INTO order_items (order_items_id, order_id, product_no, quantity) VALUES
    (1, 1, 1, 5),
    (2, 1, 2, 10),
    (3, 2, 1, 7);
INSERT INTO customer_orders (customer_orders_id, customer_id, order_id) VALUES
    (1, 1, 1),
    (2, 2, 2);
INSERT INTO customer_wishlists (customer_wishlist_id, customer_id, wishlist_id) VALUES
    (1, 2, 3),
    (2, 3, 1),
    (3, 3, 2);
INSERT INTO wishlist_items (wishlist_items_id, wishlist_id, product_no) VALUES
    (1, 1, 2),
    (2, 1, 3),
    (3, 2, 1),
    (4, 3, 1);

-- single element path pattern
SELECT * FROM GRAPH_TABLE (myshop MATCH (c IS customers) COLUMNS (c.name));
SELECT * FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name));
-- graph element specification without label or variable
SELECT * FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[]->(o IS orders) COLUMNS (c.name AS customer_name));
SELECT * FROM GRAPH_TABLE (myshop MATCH (c:customers)-[co:customer_orders]->(o:orders WHERE o.ordered_when = date '2024-01-02') COLUMNS (c.name, c.address));
SELECT * FROM GRAPH_TABLE (myshop MATCH (o IS orders)-[IS customer_orders]->(c IS customers) COLUMNS (c.name, o.ordered_when));
SELECT * FROM GRAPH_TABLE (myshop MATCH (o IS orders)<-[IS customer_orders]-(c IS customers) COLUMNS (c.name, o.ordered_when));
SELECT * FROM GRAPH_TABLE (myshop MATCH ( o IS orders ) <- [ IS customer_orders ] - (c IS customers) COLUMNS ( c.name, o.ordered_when));
SELECT * FROM GRAPH_TABLE (myshop MATCH (c IS customers)-[IS cust_lists]->(l IS lists)-[ IS list_items]->(p IS products) COLUMNS (c.name AS customer_name, p.name as product_name, l.list_type)) ORDER BY customer_name, product_name, list_type;
-- label disjunction
SELECT * FROM GRAPH_TABLE (myshop MATCH (c IS customers)-[IS customer_orders | customer_wishlists ]->(l IS orders | wishlists)-[ IS list_items]->(p IS products) COLUMNS (c.name AS customer_name, p.name as product_name)) ORDER BY customer_name, product_name;
-- property not associated with labels queried results in error
SELECT * FROM GRAPH_TABLE (myshop MATCH (c IS customers)-[IS customer_orders | customer_wishlists ]->(l IS orders | wishlists)-[ IS list_items]->(p IS products) COLUMNS (c.name AS customer_name, p.name as product_name, l.list_type)) ORDER BY 1, 2, 3;
-- vertex to vertex connection abbreviation
SELECT * FROM GRAPH_TABLE (myshop MATCH (c IS customers)->(o IS orders) COLUMNS (c.name, o.ordered_when)) ORDER BY 1;

-- lateral test
CREATE TABLE x1 (a int, b text);
INSERT INTO x1 VALUES (1, 'one'), (2, 'two');
SELECT * FROM x1, GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US' AND c.customer_id = x1.a)-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name, c.customer_id AS cid));
DROP TABLE x1;


create table v1 (id int primary key,
					vname varchar(10),
					vprop1 int,
					vprop2 int);

create table v2 (id1 int,
					id2 int,
					vname varchar(10),
					vprop1 int,
					vprop2 int);

create table v3 (id int primary key,
					vname varchar(10),
					vprop1 int,
					vprop2 int);

-- edge connecting v1 and v2
create table e1_2 (id_1 int,
					id_2_1 int,
					id_2_2 int,
					ename varchar(10),
					eprop1 int);

-- edge connecting v1 and v3
create table e1_3 (id_1 int,
					id_3 int,
					ename varchar(10),
					eprop1 int,
					primary key (id_1, id_3));

create table e2_3 (id_2_1 int,
                    id_2_2 int,
                    id_3 int,
                    ename varchar(10),
                    eprop1 int);

create property graph g1
vertex tables (
	v1
        label vl1 properties (vname, vprop1)
        label l1 properties (vname as elname), -- label shared by vertexes as well as edges
	v2 key (id1, id2)
		label vl2 properties (vname, vprop2, 'vl2_prop'::varchar(10) as lprop1)
        label vl3 properties (vname, vprop1, 'vl2_prop'::varchar(10) as lprop1)
        label l1 properties (vname as elname),
	v3
		label vl3 properties (vname, vprop1, 'vl3_prop'::varchar(10) as lprop1)
        label l1 properties (vname as elname)
)
-- edges with differing number of columns in destination keys
edge tables (
	e1_2 key (id_1, id_2_1, id_2_2)
		source key (id_1) references v1 (id)
		destination key (id_2_1, id_2_2) references v2 (id1, id2)
		label el1 properties (eprop1, ename)
        label l1 properties (ename as elname),
	e1_3
		source key (id_1) references v1 (id)
		destination key (id_3) references v3 (id)
		-- order of property names doesn't matter
		label el1 properties (ename, eprop1)
        label l1 properties (ename as elname),
    e2_3 key (id_2_1, id_2_2, id_3)
        source key (id_2_1, id_2_2) references v2 (id1, id2)
        destination key (id_3) references v3 (id)
        -- new property lprop2 not shared by el1
        -- does not share eprop1 from by el1
        label el2 properties (ename, eprop1 * 10 as lprop2)
        label l1 properties (ename as elname)
);

insert into v1 values (1, 'v11', 10, 100),
                      (2, 'v12', 20, 200),
                      (3, 'v13', 30, 300);

insert into v2 values (1000, 1, 'v21', 1010, 1100),
                      (1000, 2, 'v22', 1020, 1200),
                      (1000, 3, 'v23', 1030, 1300);

insert into v3 values (2001, 'v31', 2010, 2100),
                      (2002, 'v32', 2020, 2200),
                      (2003, 'v33', 2030, 2300);

insert into e1_2 values (1, 1000, 2, 'e121', 10001),
                        (2, 1000, 1, 'e122', 10002);

insert into e1_3 values (1, 2003, 'e131', 10003),
                        (1, 2001, 'e132', 10004);
insert into e2_3 values (1000, 2, 2002, 'e231', 10005);

-- empty element path pattern, counts number of edges in the graph
SELECT count(*) FROM GRAPH_TABLE (g1 MATCH ()-[]->() COLUMNS (1 as one));
SELECT count(*) FROM GRAPH_TABLE (g1 MATCH ()->() COLUMNS (1 as one));
-- Vertex element v2 has label vl3 which exposes property vprop1. But vl3 is
-- not part of label expression. Instead v2 get bound through label vl2 which
-- does not expose vprop1. The GRAPH_TABLE clause project vprop1.
--
-- TODO: This case fails since catalogs do not associated properties with
-- elements directly. More code is needed to make it work.
SELECT * FROM GRAPH_TABLE (g1 MATCH (a IS vl1 | vl2) COLUMNS (a.vname,
a.vprop1));
-- vprop2 is associated with vl2 but not vl3
select src, conn, dest, lprop1, vprop2, vprop1 from graph_table (g1 match (a is vl1)-[b is el1]->(c is vl2 | vl3) columns (a.vname as src, b.ename as conn, c.vname as dest, c.lprop1, c.vprop2, c.vprop1));

-- Errors
-- vl1 is not associated with property vprop2
select src, src_vprop2, conn, dest from graph_table (g1 match (a is vl1)-[b is el1]->(c is vl2 | vl3) columns (a.vname as src, a.vprop2 as src_vprop2, b.ename as conn, c.vname as dest));
-- property ename is associated with edge labels but not with a vertex label
select * from graph_table (g1 match (src)-[conn]->(dest) columns (src.vname as svname, src.ename as sename));
-- vname is associated vertex labels but not an edge label
select * from graph_table (g1 match (src)-[conn]->(dest) columns (conn.vname as cvname, conn.ename as cename));
-- el1 is associated with edges but is only label used to qualify vertex
select * from graph_table (g1 match (src is el1)-[conn]->(dest) columns (conn.ename as cename));
-- el1 is associated with edges but is one of the labels used to qualify vertex
select * from graph_table (g1 match (src is el1 | vl1)-[conn]->(dest) columns (conn.ename as cename));

-- select all the properties across all the labels associated with a given type
-- of graph element
select * from graph_table (g1 match (src)-[conn]->(dest) columns (src.vname as svname, conn.ename as cename, dest.vname as dvname, src.vprop1 as svp1, src.vprop2 as svp2, src.lprop1 as slp1, dest.vprop1 as dvp1, dest.vprop2 as dvp2, dest.lprop1 as dlp1, conn.eprop1 as cep1, conn.lprop2 as clp2));
-- three label disjunction
select * from graph_table (g1 match (src IS vl1 | vl2 | vl3)-[conn]->(dest) columns (src.vname as svname, conn.ename as cename, dest.vname as dvname));
-- graph'ical query: find a vertex which is not connected to any other vertex as a source or a destination.
with all_connected_vertices as (select svn, dvn from graph_table (g1 match (src)-[conn]->(dest) columns (src.vname as svn, dest.vname as dvn))),
    all_vertices as (select vn from graph_table (g1 match (vertex) columns (vertex.vname as vn)))
select vn from all_vertices except (select svn from all_connected_vertices union select dvn from all_connected_vertices);
-- query all connections using a label shared by vertices and edges
select sn, cn, dn from graph_table (g1 match (src : l1)-[conn : l1]->(dest : l1) columns (src.elname as sn, conn.elname as cn, dest.elname as dn));

-- property graph with some of the elements, labels and properties same as the
-- previous one. Test whether components from the specified property graph are
-- used.
create property graph g2
vertex tables (
	v1
        label l1 properties ('g2.' || vname as elname),
	v2 key (id1, id2)
        label l1 properties ('g2.' || vname as elname),
	v3
        label l1 properties ('g2.' || vname as elname)
)
edge tables (
	e1_2 key (id_1, id_2_1, id_2_2)
		source key (id_1) references v1 (id)
		destination key (id_2_1, id_2_2) references v2 (id1, id2)
        label l1 properties ('g2.' || ename as elname),
	e1_3
		source key (id_1) references v1 (id)
		destination key (id_3) references v3 (id)
        label l1 properties ('g2.' || ename as elname),
    e2_3 key (id_2_1, id_2_2, id_3)
        source key (id_2_1, id_2_2) references v2 (id1, id2)
        destination key (id_3) references v3 (id)
        label l1 properties ('g2.' || ename as elname)
);
select sn, cn, dn from graph_table (g2 match (src : l1)-[conn : l1]->(dest : l1) columns (src.elname as sn, conn.elname as cn, dest.elname as dn));

CREATE VIEW customers_us AS SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));

SELECT pg_get_viewdef('customers_us'::regclass);

-- test view/graph nesting

CREATE VIEW customers_view AS SELECT customer_id, 'redacted' || customer_id AS name_redacted, address FROM customers;
SELECT * FROM customers;
SELECT * FROM customers_view;

CREATE PROPERTY GRAPH myshop2
    VERTEX TABLES (
        products,
        customers_view KEY (customer_id) LABEL customers,
        orders
    )
    EDGE TABLES (
        order_items KEY (order_items_id)
            SOURCE KEY (order_id) REFERENCES orders (order_id)
            DESTINATION KEY (product_no) REFERENCES products (product_no),
        customer_orders KEY (customer_orders_id)
            SOURCE KEY (customer_id) REFERENCES customers_view (customer_id)
            DESTINATION KEY (order_id) REFERENCES orders (order_id)
    );

CREATE VIEW customers_us_redacted AS SELECT * FROM GRAPH_TABLE (myshop2 MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name_redacted AS customer_name_redacted));

SELECT * FROM customers_us_redacted;

-- leave for pg_upgrade/pg_dump tests
--DROP SCHEMA graph_table_tests CASCADE;
