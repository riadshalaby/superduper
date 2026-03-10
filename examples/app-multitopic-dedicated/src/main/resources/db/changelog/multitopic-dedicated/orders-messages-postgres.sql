CREATE TABLE IF NOT EXISTS orders_messages (
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

CREATE INDEX IF NOT EXISTS idx_orders_messages_topic_status_key_id
    ON orders_messages (topic, status, message_key, id);

CREATE INDEX IF NOT EXISTS idx_orders_messages_processing_worker_key_id
    ON orders_messages (topic, container_id, message_key, id)
    WHERE status = 'PROCESSING';

CREATE INDEX IF NOT EXISTS idx_orders_messages_processing_last_updated
    ON orders_messages (topic, last_updated)
    WHERE status = 'PROCESSING';
