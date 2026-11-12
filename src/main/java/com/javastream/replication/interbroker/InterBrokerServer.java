package com.javastream.replication.interbroker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** TCP acceptor for inter-broker replication and cluster coordination RPCs. */
public final class InterBrokerServer {

    private static final Logger log = LoggerFactory.getLogger(InterBrokerServer.class);

    private final String listenHost;
    private final int listenPort;
    private final int socketTimeoutMs;
    private final InterBrokerRequestHandler handler;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Set<Socket> activeSockets = ConcurrentHashMap.newKeySet();

    private ServerSocket serverSocket;
    private Thread acceptorThread;

    public InterBrokerServer(String listenHost, int listenPort, InterBrokerRequestHandler handler) {
        this(listenHost, listenPort, 30_000, handler);
    }

    public InterBrokerServer(String listenHost, int listenPort, int socketTimeoutMs, InterBrokerRequestHandler handler) {
        this.listenHost = listenHost;
        this.listenPort = listenPort;
        this.socketTimeoutMs = socketTimeoutMs;
        this.handler = handler;
    }

    public int boundPort() {
        if (serverSocket == null || !serverSocket.isBound()) {
            return -1;
        }
        return serverSocket.getLocalPort();
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            if (listenPort == 0) {
                serverSocket.bind(new InetSocketAddress(listenHost, 0));
            } else {
                serverSocket.bind(new InetSocketAddress(listenHost, listenPort));
            }
        } catch (IOException e) {
            running.set(false);
            throw new UncheckedIOException("Failed to bind inter-broker listener on " + listenHost + ":" + listenPort, e);
        }
        log.info("Inter-broker server listening on {}:{}", listenHost, boundPort());
        acceptorThread = Thread.ofVirtual().name("interbroker-acceptor").start(this::acceptLoop);
    }

    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket socket = serverSocket.accept();
                socket.setSoTimeout(socketTimeoutMs);
                socket.setTcpNoDelay(true);
                activeSockets.add(socket);
                Thread.ofVirtual().name("interbroker-client").start(() -> handleSocket(socket));
            } catch (IOException e) {
                if (running.get()) {
                    log.warn("Inter-broker accept error: {}", e.toString());
                }
            }
        }
    }

    private void handleSocket(Socket socket) {
        try (socket) {
            InputStream input = socket.getInputStream();
            OutputStream output = socket.getOutputStream();
            byte[] headerBytes = input.readNBytes(InterBrokerConstants.HEADER_BYTES);
            if (headerBytes.length != InterBrokerConstants.HEADER_BYTES) {
                return;
            }
            ByteBuffer header = ByteBuffer.wrap(headerBytes).order(ByteOrder.BIG_ENDIAN);
            if (header.getInt() != InterBrokerConstants.MAGIC) {
                return;
            }
            if (header.getShort() != InterBrokerConstants.VERSION) {
                return;
            }
            int correlation = header.getInt();
            InterBrokerOperation operation = InterBrokerOperation.fromWire(header.getShort());
            int length = header.getInt();
            byte[] payload = length == 0 ? new byte[0] : input.readNBytes(length);

            short status = 0;
            byte[] responsePayload;
            try {
                responsePayload = handler.handle(operation, payload);
            } catch (Exception e) {
                log.debug("Inter-broker request {} failed: {}", operation, e.toString());
                status = 1;
                responsePayload = new byte[0];
            }
            writeResponse(output, correlation, status, responsePayload);
        } catch (IOException e) {
            log.debug("Inter-broker connection closed: {}", e.toString());
        } finally {
            activeSockets.remove(socket);
        }
    }

    private static void writeResponse(OutputStream output, int correlation, short status, byte[] payload)
            throws IOException {
        ByteBuffer header =
                ByteBuffer.allocate(InterBrokerConstants.HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        header.putInt(InterBrokerConstants.MAGIC);
        header.putShort(InterBrokerConstants.VERSION);
        header.putInt(correlation);
        header.putShort(status);
        header.putInt(payload.length);
        output.write(header.array());
        if (payload.length > 0) {
            output.write(payload);
        }
        output.flush();
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
        }
        for (Socket socket : activeSockets) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
        activeSockets.clear();
        if (acceptorThread != null) {
            try {
                acceptorThread.join(3_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
