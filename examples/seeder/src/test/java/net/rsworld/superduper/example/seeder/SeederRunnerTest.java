package net.rsworld.superduper.example.seeder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SeederRunnerTest {

    @Test
    void effectiveKeyCountUsesAtLeastTenPercentOfMessageCount() {
        assertEquals(100, SeederRunner.effectiveKeyCount(1000, 20));
        assertEquals(200, SeederRunner.effectiveKeyCount(1000, 200));
        assertEquals(2, SeederRunner.effectiveKeyCount(11, 1));
    }

    @Test
    void failureMessagesStayOnSingleDedicatedKey() {
        int effectiveKeys = SeederRunner.effectiveKeyCount(1000, 20);
        Set<String> failureKeys = new HashSet<>();
        Set<String> normalKeys = new HashSet<>();

        for (int i = 1; i <= 1000; i++) {
            String key = SeederRunner.keyFor(i, effectiveKeys);
            if (SeederRunner.isFailureIndex(i)) {
                failureKeys.add(key);
            } else {
                normalKeys.add(key);
            }
        }

        assertEquals(Set.of("order-0"), failureKeys);
        assertTrue(normalKeys.stream().noneMatch("order-0"::equals));
        assertEquals(99, normalKeys.size());
    }
}
