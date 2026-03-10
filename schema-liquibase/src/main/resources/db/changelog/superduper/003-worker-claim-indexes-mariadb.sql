DROP INDEX IF EXISTS idx_messages_message_key_id ON messages;

CREATE INDEX IF NOT EXISTS idx_messages_topic_status_key_id
    ON messages (topic, status, message_key, id);

CREATE INDEX IF NOT EXISTS idx_messages_processing_worker_status_container_key_id
    ON messages (status, container_id(191), topic(191), message_key(191), id);

CREATE INDEX IF NOT EXISTS idx_messages_processing_stale_status_last_updated
    ON messages (topic, status, last_updated);
