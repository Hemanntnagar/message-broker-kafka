package com.javastream.consumer;

import com.javastream.message.Message;

import java.io.IOException;

/** Processes a single consumed record; throw {@link RetryableProcessingException} for retriable failures. */
@FunctionalInterface
public interface MessageHandler {

    void handle(Message message) throws IOException, RetryableProcessingException;
}
