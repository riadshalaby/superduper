CREATE TABLE IF NOT EXISTS ${table.name} (
  id BIGSERIAL PRIMARY KEY,
  topic VARCHAR(255) NOT NULL DEFAULT 'default',
  message_id VARCHAR(36) UNIQUE NOT NULL,
  message_key VARCHAR(255) NOT NULL,
  content TEXT,
  status VARCHAR(32) NOT NULL CHECK (status IN ('READY','PROCESSING','PROCESSED','FAILED','STOPPED')),
  retry_count INT DEFAULT 0,
  container_id VARCHAR(255),
  correlation_id VARCHAR(36) NULL,
  message_type VARCHAR(255) NULL,
  occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  processed_at TIMESTAMP NULL,
  last_updated TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_${table.name}_topic_status_key_id
    ON ${table.name} (topic, status, message_key, id);

CREATE INDEX IF NOT EXISTS idx_${table.name}_processing_worker_key_id
    ON ${table.name} (topic, container_id, message_key, id)
    WHERE status = 'PROCESSING';

CREATE INDEX IF NOT EXISTS idx_${table.name}_processing_last_updated
    ON ${table.name} (topic, last_updated)
    WHERE status = 'PROCESSING';
