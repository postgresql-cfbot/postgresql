CREATE TABLE simple_table(
    id int,
    name varchar(5)
);

-- Test TableLikeClause is handled properly
CREATE TABLE ctlt1 (a text CHECK (length(a) > 2) PRIMARY KEY, b text);
ALTER TABLE ctlt1 ALTER COLUMN a SET STORAGE MAIN;
ALTER TABLE ctlt1 ALTER COLUMN b SET STORAGE EXTERNAL;
CREATE TABLE ctlt1_like (LIKE ctlt1 INCLUDING ALL);

-- Test foreign key constraint is handled in a following ALTER TABLE ADD CONSTRAINT FOREIGN KEY REFERENCES subcommand
CREATE TABLE product (id int PRIMARY KEY, name text);
CREATE TABLE orders (order_id int PRIMARY KEY, product_id int
REFERENCES product (id));
