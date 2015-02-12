SELECT slot_name FROM pg_create_logical_replication_slot('custom_slot', 'decoder_json');

-- DEFAULT case with PRIMARY KEY
CREATE TABLE aa (a int primary key, b text NOT NULL);
INSERT INTO aa VALUES (1, 'aa'), (2, 'что то');

SELECT data FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'off');
SELECT pg_drop_replication_slot('custom_slot');