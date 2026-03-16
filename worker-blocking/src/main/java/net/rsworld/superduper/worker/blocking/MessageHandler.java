package net.rsworld.superduper.worker.blocking;

/** Handles a claimed message in the blocking worker runtime. */
public interface MessageHandler {
    /**
     * Processes a claimed message and returns the resulting outcome.
     *
     * @param row the claimed message to handle
     * @return the processing outcome for the message
     * @throws MessageHandlingException when the handler cannot complete processing
     */
    ProcessingResult handle(MessageRow row) throws MessageHandlingException;
}
