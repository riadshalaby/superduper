package net.rsworld.superduper.worker.reactive;

import reactor.core.publisher.Mono;

/** Handles a claimed message in the reactive worker runtime. */
public interface ReactiveMessageHandler {
    /**
     * Processes a claimed message asynchronously and emits the resulting outcome.
     *
     * @param row the claimed message to handle
     * @return a publisher that emits the processing outcome
     */
    Mono<ProcessingResult> handle(MessageRow row);
}
