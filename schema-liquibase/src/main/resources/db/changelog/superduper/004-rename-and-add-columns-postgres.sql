ALTER TABLE messages RENAME COLUMN uuid TO message_id;
ALTER TABLE messages RENAME COLUMN key TO message_key;
ALTER TABLE messages ADD COLUMN correlation_id VARCHAR(36) NULL;
ALTER TABLE messages ADD COLUMN message_type VARCHAR(255) NULL;

ALTER INDEX IF EXISTS idx_messages_key_id RENAME TO idx_messages_message_key_id;
