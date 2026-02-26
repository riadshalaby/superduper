package net.rsworld.superduper.worker.blocking;

public interface MessageHandler {
    ProcessingResult handle(MessageRow row) throws MessageHandlingException;
}
