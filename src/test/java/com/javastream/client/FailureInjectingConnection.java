package com.javastream.client;

import com.javastream.protocol.OperationCode;
import com.javastream.protocol.Response;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test double that fails the first N {@link #send} calls with {@link IOException}.
 */
public final class FailureInjectingConnection extends TcpProtocolConnection {

    private final AtomicInteger sendAttempts = new AtomicInteger();
    private final int failUntilAttempt;
    private final TcpProtocolConnection delegate;

    public FailureInjectingConnection(TcpProtocolConnection delegate, int failUntilAttempt) {
        super("127.0.0.1", 1);
        this.delegate = delegate;
        this.failUntilAttempt = failUntilAttempt;
    }

    @Override
    public synchronized void connect() throws IOException {
        delegate.connect();
    }

    @Override
    public boolean isConnected() {
        return delegate.isConnected();
    }

    @Override
    public synchronized Response send(OperationCode operation, byte[] payload) throws IOException {
        return send(operation, payload, null);
    }

    @Override
    public synchronized Response send(OperationCode operation, byte[] payload, Duration requestTimeout)
            throws IOException {
        int attempt = sendAttempts.incrementAndGet();
        if (operation == OperationCode.PRODUCE && attempt <= failUntilAttempt) {
            delegate.close();
            throw new IOException("injected network failure on attempt " + attempt);
        }
        if (!delegate.isConnected()) {
            delegate.connect();
        }
        return delegate.send(operation, payload, requestTimeout);
    }

    @Override
    public synchronized void close() {
        delegate.close();
    }
}
