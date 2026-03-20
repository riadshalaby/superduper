package net.rsworld.superduper.starter;

import java.util.Collections;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "superduper")
public class OutboxProperties {
    private Map<String, OutboxConfig> outbox = Collections.emptyMap();

    public Map<String, OutboxConfig> getOutbox() {
        return outbox;
    }

    public void setOutbox(Map<String, OutboxConfig> outbox) {
        this.outbox = outbox == null ? Collections.emptyMap() : outbox;
    }

    public static class OutboxConfig {
        private String handler;
        private int batchSize = 0;
        private int maxRetries = 0;
        private String table = "";

        public String getHandler() {
            return handler;
        }

        public void setHandler(String handler) {
            this.handler = handler;
        }

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

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table == null ? "" : table;
        }
    }
}
