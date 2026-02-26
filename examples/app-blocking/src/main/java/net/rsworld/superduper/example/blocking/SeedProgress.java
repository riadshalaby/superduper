package net.rsworld.superduper.example.blocking;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

@Component
class SeedProgress {
    private final AtomicInteger expectedCount = new AtomicInteger();
    private final Set<Long> seenIds = ConcurrentHashMap.newKeySet();
    private volatile String activeRunToken = "";
    private final AtomicInteger handledCount = new AtomicInteger();
    private volatile CountDownLatch latch = new CountDownLatch(0);

    void startRun(int expectedCount, String runToken) {
        this.activeRunToken = runToken == null ? "" : runToken;
        this.expectedCount.set(expectedCount);
        this.seenIds.clear();
        this.handledCount.set(0);
        this.latch = new CountDownLatch(expectedCount);
    }

    int expectedCount() {
        return expectedCount.get();
    }

    int handledCount() {
        return handledCount.get();
    }

    String activeRunToken() {
        return activeRunToken;
    }

    void markHandled(Long id, String runToken) {
        if (runToken != null && runToken.equals(activeRunToken) && id != null && seenIds.add(id)) {
            handledCount.incrementAndGet();
            latch.countDown();
        }
    }

    boolean await(long timeoutSeconds) throws InterruptedException {
        return latch.await(timeoutSeconds, TimeUnit.SECONDS);
    }
}
