CREATE TABLE IF NOT EXISTS messages (
  id BIGSERIAL PRIMARY KEY,
  uuid VARCHAR(36) UNIQUE NOT NULL,
  key VARCHAR(255) NOT NULL,
  content TEXT,
  status TEXT NOT NULL,
  retry_count INT DEFAULT 0,
  container_id VARCHAR(255),
  timestamp TIMESTAMP,
  last_updated TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_messages_key_id ON messages(key, id);

CREATE TABLE IF NOT EXISTS container_heartbeats (
  container_id VARCHAR(255) PRIMARY KEY,
  last_heartbeat TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS shedlock (
  name VARCHAR(64) PRIMARY KEY,
  lock_until TIMESTAMP(3) NOT NULL,
  locked_at TIMESTAMP(3) NOT NULL,
  locked_by VARCHAR(255) NOT NULL
);
