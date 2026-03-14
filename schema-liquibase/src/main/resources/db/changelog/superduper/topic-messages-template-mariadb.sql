CREATE TABLE IF NOT EXISTS ${table.name} (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  topic VARCHAR(255) NOT NULL DEFAULT 'default',
  message_id VARCHAR(36) UNIQUE NOT NULL,
  message_key VARCHAR(36) NOT NULL,
  content TEXT,
  status VARCHAR(32) NOT NULL,
  retry_count INT DEFAULT 0,
  container_id VARCHAR(255),
  correlation_id VARCHAR(36) NULL,
  message_type VARCHAR(255) NULL,
  occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  processed_at TIMESTAMP NULL,
  last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE INDEX idx_${table.name}_topic_status_key_id
    ON ${table.name} (topic, status, message_key, id);

CREATE INDEX idx_${table.name}_processing_worker_status_container_key_id
    ON ${table.name} (status, container_id, topic, message_key, id);

CREATE INDEX idx_${table.name}_processing_stale_status_last_updated
    ON ${table.name} (topic, status, last_updated);
