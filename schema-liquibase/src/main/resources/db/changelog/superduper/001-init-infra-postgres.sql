CREATE TABLE IF NOT EXISTS container_heartbeats (
  container_id VARCHAR(255) PRIMARY KEY,
  last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS shedlock (
  name VARCHAR(64) PRIMARY KEY,
  lock_until TIMESTAMP(3) NOT NULL,
  locked_at TIMESTAMP(3) NOT NULL,
  locked_by VARCHAR(255) NOT NULL
);
