SELECT slot_name FROM pg_create_logical_replication_slot('custom_slot', 'decoder_json');

-- FULL case with PRIMARY KEY
DROP TABLE IF EXISTS AA;
CREATE TABLE aa (a int primary key, b text NOT NULL);
ALTER TABLE aa WITH REPLICA IDENTITY FULL;
INSERT INTO aa VALUES (1, 'aa'), (2, 'bb');
UPDATE aa SET b = 'aa1' where a = 1; -- Primary key not changed
UPDATE aa SET a=3, b = 'aa2' where a = 1; -- Primary key changed
UPDATE aa SET b='cc' WHERE a != 0;

SELECT data FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'off', 'sort_keys', 'on');
SELECT pg_drop_replication_slot('custom_slot');