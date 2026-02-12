package net.rsworld.superduper.example.reactive;

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
    private final AtomicInteger runSequence = new AtomicInteger();
    private volatile int activeRun = 0;
    private final AtomicInteger handledCount = new AtomicInteger();
    private volatile CountDownLatch latch = new CountDownLatch(0);

    void startRun(int expectedCount) {
        this.activeRun = runSequence.incrementAndGet();
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

    int activeRun() {
        return activeRun;
    }

    void markHandled(Long id, int run) {
        if (run == activeRun && id != null && seenIds.add(id)) {
            handledCount.incrementAndGet();
            latch.countDown();
        }
    }

    boolean await(long timeoutSeconds) throws InterruptedException {
        return latch.await(timeoutSeconds, TimeUnit.SECONDS);
    }
}
