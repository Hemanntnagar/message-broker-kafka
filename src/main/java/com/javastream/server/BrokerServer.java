package com.javastream.server;

import com.javastream.broker.BrokerRequestDispatcher;
import com.javastream.broker.BrokerServices;
import com.javastream.cluster.ClusterManager;
import com.javastream.config.JavaStreamConfig;
import com.javastream.monitoring.MonitoringBundle;
import com.javastream.security.TlsServerSockets;
import com.javastream.security.TlsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TCP acceptor for the broker. One virtual thread per accepted client.
 */
public final class BrokerServer {

    public static final int DEFAULT_LISTEN_PORT = 9092;
    public static final int DEFAULT_SOCKET_TIMEOUT_MS = 30_000;
    public static final int DEFAULT_MAX_REQUEST_BYTES = 1_048_576;
    public static final int DEFAULT_BACKLOG = 128;

    private static final Logger log = LoggerFactory.getLogger(BrokerServer.class);

    private final String listenHost;
    private final int listenPort;
    private final int socketTimeoutMs;
    private final int maxRequestBytes;
    private final int backlog;
    private final int maxConcurrentConnections;
    private final BrokerRequestDispatcher dispatcher;
    private final MonitoringBundle monitoring;
    private final TlsSettings tlsSettings;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Set<ClientHandler> activeHandlers = ConcurrentHashMap.newKeySet();

    private ServerSocket serverSocket;
    private Thread acceptorThread;

    public BrokerServer(JavaStreamConfig config) {
        this(config, new BrokerServices(config));
    }

    public BrokerServer(JavaStreamConfig config, BrokerServices brokerServices) {
        this(config, brokerServices, java.util.Optional.empty());
    }

    public BrokerServer(JavaStreamConfig config, BrokerServices brokerServices, java.util.Optional<ClusterManager> cluster) {
        MonitoringBundle monitoring =
                MonitoringBundle.create(config, brokerServices, cluster, config.listenPort());
        this.monitoring = monitoring;
        this.listenHost = config.listenHost();
        this.listenPort = config.listenPort();
        this.socketTimeoutMs = DEFAULT_SOCKET_TIMEOUT_MS;
        this.maxRequestBytes = config.securitySettings().maxRequestBytes();
        this.maxConcurrentConnections = config.securitySettings().maxConcurrentConnections();
        this.backlog = DEFAULT_BACKLOG;
        this.tlsSettings = config.securitySettings().tls();
        this.dispatcher =
                new BrokerRequestDispatcher(
                        brokerServices,
                        monitoring.metrics(),
                        monitoring.adminAuthority(),
                        monitoring.adminService(),
                        config.securitySettings());
    }

    BrokerServer(
            String listenHost,
            int listenPort,
            int socketTimeoutMs,
            int maxRequestBytes,
            int backlog) {
        this(
                listenHost,
                listenPort,
                socketTimeoutMs,
                maxRequestBytes,
                backlog,
                new BrokerRequestDispatcher(new BrokerServices()));
    }

    BrokerServer(
            String listenHost,
            int listenPort,
            int socketTimeoutMs,
            int maxRequestBytes,
            int backlog,
            BrokerRequestDispatcher dispatcher) {
        this.listenHost = listenHost;
        this.listenPort = listenPort;
        this.socketTimeoutMs = socketTimeoutMs;
        this.maxRequestBytes = maxRequestBytes;
        this.backlog = backlog;
        this.maxConcurrentConnections = Integer.MAX_VALUE;
        this.dispatcher = dispatcher;
        this.monitoring = null;
        this.tlsSettings = TlsSettings.disabled();
    }

    public BrokerRequestDispatcher dispatcher() {
        return dispatcher;
    }

    /** Starts a broker with explicit backing services (load tests and integration harnesses). */
    public static BrokerServer forLoadTest(String listenHost, int listenPort, BrokerServices brokerServices) {
        return new BrokerServer(
                listenHost,
                listenPort,
                DEFAULT_SOCKET_TIMEOUT_MS,
                DEFAULT_MAX_REQUEST_BYTES,
                256,
                new BrokerRequestDispatcher(brokerServices));
    }

    public String listenHost() {
        return listenHost;
    }

    public int listenPort() {
        return listenPort;
    }

    public int boundPort() {
        ServerSocket socket = serverSocket;
        if (socket == null || !socket.isBound()) {
            return -1;
        }
        return socket.getLocalPort();
    }

    public boolean isRunning() {
        return running.get();
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("BrokerServer already running on {}:{}", listenHost, listenPort);
            return;
        }

        try {
            if (tlsSettings.enabled()) {
                serverSocket = TlsServerSockets.openBound(tlsSettings, listenHost, listenPort, backlog);
            } else {
                serverSocket = new ServerSocket();
                serverSocket.setReuseAddress(true);
                serverSocket.bind(new InetSocketAddress(listenHost, listenPort), backlog);
            }
        } catch (IOException e) {
            running.set(false);
            throw new UncheckedIOException(
                    "Failed to bind TCP listener on %s:%d".formatted(listenHost, listenPort), e);
        }

        log.info(
                "BrokerServer listening on {}:{} (tls={}, socketTimeoutMs={}, maxRequestBytes={}, maxConnections={})",
                listenHost,
                boundPort(),
                tlsSettings.enabled(),
                socketTimeoutMs,
                maxRequestBytes,
                maxConcurrentConnections);

        if (monitoring != null) {
            monitoring.attach(this);
            monitoring.setReady(true);
            monitoring.startHttp();
        }

        acceptorThread = Thread.ofVirtual().name("broker-acceptor").start(this::acceptLoop);
    }

    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket clientSocket = serverSocket.accept();
                if (activeHandlers.size() >= maxConcurrentConnections) {
                    log.warn("Rejecting connection: maxConcurrentConnections={}", maxConcurrentConnections);
                    clientSocket.close();
                    continue;
                }
                configureClientSocket(clientSocket);
                ClientHandler handler = new ClientHandler(
                        clientSocket, socketTimeoutMs, maxRequestBytes, activeHandlers::remove, dispatcher);
                activeHandlers.add(handler);
                Thread.ofVirtual().name("broker-client-" + clientSocket.getPort()).start(handler);
            } catch (IOException e) {
                if (running.get()) {
                    log.warn("Accept loop I/O error (server will continue if still running): {}", e.toString());
                } else {
                    log.debug("Accept loop stopped: {}", e.toString());
                }
            }
        }
    }

    private void configureClientSocket(Socket socket) throws IOException {
        socket.setSoTimeout(socketTimeoutMs);
        socket.setTcpNoDelay(true);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        log.info("Stopping BrokerServer on {}:{}", listenHost, listenPort);

        if (monitoring != null) {
            monitoring.setReady(false);
            monitoring.close();
        }

        IOException closeError = null;
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                closeError = e;
            }
        }

        for (ClientHandler handler : activeHandlers) {
            handler.closeQuietly();
        }
        activeHandlers.clear();

        if (acceptorThread != null) {
            try {
                acceptorThread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (closeError != null) {
            log.warn("Error while closing server socket: {}", closeError.toString());
        }

        log.info("BrokerServer stopped");
    }

    public int activeConnectionCount() {
        return activeHandlers.size();
    }

    public MonitoringBundle monitoring() {
        return monitoring;
    }
}
