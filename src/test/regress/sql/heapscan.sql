-- make sure that initially the table is empty
SELECT * FROM phonebook;

SELECT phonebook_find_first_phone(isnull => false);
SELECT phonebook_find_first_phone(isnull => true);

INSERT INTO phonebook (id, name, phone) VALUES
(1, 'Alice', 123456),
(2, 'Bob', NULL);

SELECT phonebook_find_first_phone(isnull => false);
SELECT phonebook_find_first_phone(isnull => true);