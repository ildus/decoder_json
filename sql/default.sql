-- SET UP
SELECT slot_name FROM pg_create_logical_replication_slot('custom_slot', 'decoder_json');

-- DEFAULT case with PRIMARY KEY
CREATE TABLE aa (a int primary key, b text NOT NULL);
INSERT INTO aa VALUES (1, 'aa'), (2, 'bb');
UPDATE aa SET b = 'aa1' where a = 1; -- Primary key not changed
UPDATE aa SET a=3, b = 'aa2' where a = 1; -- Primary key changed
UPDATE aa SET b='cc' WHERE a != 0;

SELECT data FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'off', 'sort_keys', 'on');
DROP TABLE aa;

-- DEFAULT case without PRIMARY KEY
CREATE TABLE aa (a int, b text NOT NULL);
INSERT INTO aa VALUES (1, 'aa'), (2, 'bb');
UPDATE aa SET b = 'aa1' where a = 1;
UPDATE aa SET a=3, b = 'aa2' where a = 1;
UPDATE aa SET b='cc' WHERE a != 0;

SELECT data FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'off', 'sort_keys', 'on');
DROP TABLE aa;

-- TESTING VARIOUS TYPES
CREATE TABLE aa (a int primary key, b text, c int null, d numeric(6,2), e char, f varchar(10), g boolean, h text[], i varchar[], j int[]);
INSERT INTO aa VALUES (1, 'aa', null, '1255.56', 'e', 'text1', true, array['one', 'two'], array['three', 'four'], array[1,2]);

SELECT data FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'off', 'sort_keys', 'on');
DROP TABLE aa;

-- TEAR DOWN
SELECT pg_drop_replication_slot('custom_slot');