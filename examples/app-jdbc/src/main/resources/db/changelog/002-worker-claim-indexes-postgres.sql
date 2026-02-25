CREATE INDEX IF NOT EXISTS idx_messages_ready_claim_id_key
    ON messages (id, key)
    WHERE status = 'READY';

CREATE INDEX IF NOT EXISTS idx_messages_failed_claim_retry_id_key
    ON messages (retry_count, id, key)
    WHERE status = 'FAILED';

CREATE INDEX IF NOT EXISTS idx_messages_processing_key_id
    ON messages (key, id)
    WHERE status = 'PROCESSING';

CREATE INDEX IF NOT EXISTS idx_messages_processing_worker_key_id
    ON messages (container_id, key, id)
    WHERE status = 'PROCESSING';

CREATE INDEX IF NOT EXISTS idx_messages_processing_last_updated
    ON messages (last_updated)
    WHERE status = 'PROCESSING';
