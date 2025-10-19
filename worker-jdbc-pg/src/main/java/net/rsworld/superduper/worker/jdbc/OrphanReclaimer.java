package net.rsworld.superduper.worker.jdbc;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class OrphanReclaimer {
    private final NamedParameterJdbcTemplate jdbc;
    private final int orphanTimeoutSec;
    private final int heartbeatWindowSec;

    public OrphanReclaimer(
            NamedParameterJdbcTemplate jdbc,
            @Value("${superduper.worker.orphan-timeout-ms:120000}") int orphanTimeoutMs,
            @Value("${superduper.worker.heartbeat-interval-ms:30000}") int hbMs) {
        this.jdbc = jdbc;
        this.orphanTimeoutSec = orphanTimeoutMs / 1000;
        this.heartbeatWindowSec = hbMs / 1000;
    }

    @Scheduled(fixedDelayString = "${superduper.worker.orphan-timeout-ms:120000}", initialDelay = 15000)
    public void reclaim() {
        jdbc.update(
                "UPDATE messages SET status='READY', container_id=NULL WHERE status='PROCESSING' AND last_updated < (now() - (:t * INTERVAL '1 second'))",
                new MapSqlParameterSource().addValue("t", orphanTimeoutSec));

        jdbc.update(
                "UPDATE messages SET status='READY', container_id=NULL WHERE status='PROCESSING' AND (container_id IS NULL OR container_id NOT IN ("
                        + "SELECT container_id FROM container_heartbeats WHERE last_heartbeat >= (now() - (:hb * INTERVAL '1 second'))))",
                new MapSqlParameterSource().addValue("hb", heartbeatWindowSec));
    }
}
