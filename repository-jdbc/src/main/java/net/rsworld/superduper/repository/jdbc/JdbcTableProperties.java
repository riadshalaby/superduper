package net.rsworld.superduper.repository.jdbc;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "superduper.db.tables")
public class JdbcTableProperties {
    private String messages = "messages";
    private String heartbeats = "container_heartbeats";

    public String getMessages() {
        return messages;
    }

    public void setMessages(String messages) {
        this.messages = messages;
    }

    public String getHeartbeats() {
        return heartbeats;
    }

    public void setHeartbeats(String heartbeats) {
        this.heartbeats = heartbeats;
    }
}
