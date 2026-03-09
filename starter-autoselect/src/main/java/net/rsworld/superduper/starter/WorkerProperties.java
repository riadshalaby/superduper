package net.rsworld.superduper.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "superduper.worker")
public class WorkerProperties {
    private int batchSize = 100;
    private int maxRetries = 5;
    private long claimIntervalMs = 5000;
    private long claimInitialDelayMs = 3000;
    private long heartbeatIntervalMs = 30000;
    private long heartbeatWindowMs = 90000;
    private long heartbeatInitialDelayMs = 0;
    private int orphanTimeoutMs = 120000;
    private long orphanInitialDelayMs = 15000;
    private final Shedlock shedlock = new Shedlock();
    private final QueueHealth queueHealth = new QueueHealth();
    private final Retention retention = new Retention();

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getClaimIntervalMs() {
        return claimIntervalMs;
    }

    public void setClaimIntervalMs(long claimIntervalMs) {
        this.claimIntervalMs = claimIntervalMs;
    }

    public long getClaimInitialDelayMs() {
        return claimInitialDelayMs;
    }

    public void setClaimInitialDelayMs(long claimInitialDelayMs) {
        this.claimInitialDelayMs = claimInitialDelayMs;
    }

    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public long getHeartbeatWindowMs() {
        return heartbeatWindowMs;
    }

    public void setHeartbeatWindowMs(long heartbeatWindowMs) {
        this.heartbeatWindowMs = heartbeatWindowMs;
    }

    public long getHeartbeatInitialDelayMs() {
        return heartbeatInitialDelayMs;
    }

    public void setHeartbeatInitialDelayMs(long heartbeatInitialDelayMs) {
        this.heartbeatInitialDelayMs = heartbeatInitialDelayMs;
    }

    public int getOrphanTimeoutMs() {
        return orphanTimeoutMs;
    }

    public void setOrphanTimeoutMs(int orphanTimeoutMs) {
        this.orphanTimeoutMs = orphanTimeoutMs;
    }

    public long getOrphanInitialDelayMs() {
        return orphanInitialDelayMs;
    }

    public void setOrphanInitialDelayMs(long orphanInitialDelayMs) {
        this.orphanInitialDelayMs = orphanInitialDelayMs;
    }

    public Shedlock getShedlock() {
        return shedlock;
    }

    public QueueHealth getQueueHealth() {
        return queueHealth;
    }

    public Retention getRetention() {
        return retention;
    }

    public static class Shedlock {
        private String tableName = "shedlock";
        private String claimLockName = "superduper-claim-batch";
        private long lockAtMostForMs = 15000;
        private long lockAtLeastForMs = 2000;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getClaimLockName() {
            return claimLockName;
        }

        public void setClaimLockName(String claimLockName) {
            this.claimLockName = claimLockName;
        }

        public long getLockAtMostForMs() {
            return lockAtMostForMs;
        }

        public void setLockAtMostForMs(long lockAtMostForMs) {
            this.lockAtMostForMs = lockAtMostForMs;
        }

        public long getLockAtLeastForMs() {
            return lockAtLeastForMs;
        }

        public void setLockAtLeastForMs(long lockAtLeastForMs) {
            this.lockAtLeastForMs = lockAtLeastForMs;
        }
    }

    public static class QueueHealth {
        private boolean enabled = false;
        private long intervalMs = 60000;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public long getIntervalMs() {
            return intervalMs;
        }

        public void setIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
        }
    }

    /** Configuration for scheduled queue-retention cleanup. */
    public static class Retention {
        private boolean enabled = false;
        private int processedRetentionDays = 14;
        private int stoppedRetentionDays = 30;
        private int heartbeatRetentionDays = 1;
        private long intervalMs = 86400000;
        private long initialDelayMs = 60000;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getProcessedRetentionDays() {
            return processedRetentionDays;
        }

        public void setProcessedRetentionDays(int processedRetentionDays) {
            this.processedRetentionDays = processedRetentionDays;
        }

        public int getStoppedRetentionDays() {
            return stoppedRetentionDays;
        }

        public void setStoppedRetentionDays(int stoppedRetentionDays) {
            this.stoppedRetentionDays = stoppedRetentionDays;
        }

        public int getHeartbeatRetentionDays() {
            return heartbeatRetentionDays;
        }

        public void setHeartbeatRetentionDays(int heartbeatRetentionDays) {
            this.heartbeatRetentionDays = heartbeatRetentionDays;
        }

        public long getIntervalMs() {
            return intervalMs;
        }

        public void setIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
        }

        public long getInitialDelayMs() {
            return initialDelayMs;
        }

        public void setInitialDelayMs(long initialDelayMs) {
            this.initialDelayMs = initialDelayMs;
        }
    }
}
