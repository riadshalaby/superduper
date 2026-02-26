package net.rsworld.superduper.worker.blocking;

public class MessageHandlingException extends Exception {
    public MessageHandlingException(String message) {
        super(message);
    }

    public MessageHandlingException(String message, Throwable cause) {
        super(message, cause);
    }
}
