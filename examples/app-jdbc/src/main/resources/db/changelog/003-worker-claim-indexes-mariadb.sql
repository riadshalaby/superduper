CREATE INDEX idx_messages_claim_status_id_key
    ON messages (status, id, `key`);

CREATE INDEX idx_messages_claim_failed_status_retry_id_key
    ON messages (status, retry_count, id, `key`);

CREATE INDEX idx_messages_processing_exists_key_status
    ON messages (`key`, status);

CREATE INDEX idx_messages_processing_worker_status_container_key_id
    ON messages (status, container_id, `key`, id);

CREATE INDEX idx_messages_processing_stale_status_last_updated
    ON messages (status, last_updated);
