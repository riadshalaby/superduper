package net.rsworld.superduper.worker.jdbc;

public interface MessageHandler {
    ProcessingResult handle(MessageRow row) throws Exception;
}
