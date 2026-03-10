package net.rsworld.superduper.starter;

import java.util.Collections;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "superduper")
public class TopicProperties {
    private Map<String, TopicConfig> topics = Collections.emptyMap();

    public Map<String, TopicConfig> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, TopicConfig> topics) {
        this.topics = topics == null ? Collections.emptyMap() : topics;
    }

    public static class TopicConfig {
        private String kafkaTopic;
        private String handler;
        private int batchSize = -1;
        private int maxRetries = -1;
        private String table = "";

        public String getKafkaTopic() {
            return kafkaTopic;
        }

        public void setKafkaTopic(String kafkaTopic) {
            this.kafkaTopic = kafkaTopic;
        }

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
