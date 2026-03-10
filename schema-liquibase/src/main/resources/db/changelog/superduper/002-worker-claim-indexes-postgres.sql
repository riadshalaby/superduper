DROP INDEX IF EXISTS idx_messages_message_key_id;

CREATE INDEX IF NOT EXISTS idx_messages_topic_status_key_id
    ON messages (topic, status, message_key, id);

CREATE INDEX IF NOT EXISTS idx_messages_processing_worker_key_id
    ON messages (topic, container_id, message_key, id)
    WHERE status = 'PROCESSING';

CREATE INDEX IF NOT EXISTS idx_messages_processing_last_updated
    ON messages (topic, last_updated)
    WHERE status = 'PROCESSING';
