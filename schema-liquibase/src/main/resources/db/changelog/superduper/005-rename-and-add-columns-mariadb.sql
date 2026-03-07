ALTER TABLE messages CHANGE COLUMN uuid message_id VARCHAR(36) NOT NULL;
ALTER TABLE messages CHANGE COLUMN `key` message_key VARCHAR(255) NOT NULL;
ALTER TABLE messages ADD COLUMN correlation_id VARCHAR(36) NULL;
ALTER TABLE messages ADD COLUMN message_type VARCHAR(255) NULL;

DROP INDEX idx_messages_key_id ON messages;
CREATE INDEX idx_messages_message_key_id ON messages(message_key, id);
