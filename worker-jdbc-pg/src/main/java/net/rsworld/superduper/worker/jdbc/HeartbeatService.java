package net.rsworld.superduper.worker.jdbc;

import java.lang.management.ManagementFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class HeartbeatService {
    private final NamedParameterJdbcTemplate jdbc;
    private final String workerId;

    public HeartbeatService(
            NamedParameterJdbcTemplate jdbc, @Value("${superduper.worker.heartbeat-interval-ms:30000}") long hb) {
        this.jdbc = jdbc;
        this.workerId = ManagementFactory.getRuntimeMXBean().getName();
    }

    @Scheduled(fixedRateString = "${superduper.worker.heartbeat-interval-ms:30000}")
    public void heartbeat() {
        String sql = "INSERT INTO container_heartbeats (container_id, last_heartbeat) VALUES (:cid, now()) "
                + "ON CONFLICT (container_id) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat";
        jdbc.update(sql, new MapSqlParameterSource().addValue("cid", workerId));
    }
}
