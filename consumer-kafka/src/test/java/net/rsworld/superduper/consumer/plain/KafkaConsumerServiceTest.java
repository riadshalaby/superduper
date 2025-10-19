package net.rsworld.superduper.consumer.plain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.support.Acknowledgment;

class KafkaConsumerServiceTest {

    @Test
    void onMessage_insertsAndAcks() {
        NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
        KafkaConsumerService svc = new KafkaConsumerService(jdbc);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, String> rec = new ConsumerRecord<>("t", 0, 0L, "key1", "value1");

        when(jdbc.update(any(String.class), any())).thenReturn(1);

        svc.onMessage(rec, ack);

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(jdbc).update(sqlCap.capture(), any());
        assertThat(sqlCap.getValue()).contains("INSERT INTO messages");
        verify(ack, times(1)).acknowledge();
    }
}
