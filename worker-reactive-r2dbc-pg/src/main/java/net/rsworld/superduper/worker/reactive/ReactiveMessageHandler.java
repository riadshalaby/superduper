package net.rsworld.superduper.worker.reactive;

import reactor.core.publisher.Mono;

public interface ReactiveMessageHandler {
    Mono<ProcessingResult> handle(MessageRow row);
}
