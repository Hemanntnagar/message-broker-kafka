package com.javastream.benchmark;

import com.javastream.broker.BrokerServices;
import com.javastream.config.JavaStreamConfig;
import com.javastream.config.ConfigLoader;
import com.javastream.server.BrokerServer;
import com.javastream.storage.FileMessageStore;
import com.javastream.storage.InMemoryMessageStore;
import com.javastream.storage.log.FlushPolicy;
import com.javastream.storage.log.StorageSettings;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Properties;

/** Starts an isolated broker instance for load tests. */
public final class EmbeddedBenchmarkBroker implements AutoCloseable {

    private final BrokerServer server;
    private final java.nio.file.Path dataDir;

    private EmbeddedBenchmarkBroker(BrokerServer server, java.nio.file.Path dataDir) {
        this.server = server;
        this.dataDir = dataDir;
    }

    public static EmbeddedBenchmarkBroker start(boolean durable, FlushPolicy flushPolicy) throws IOException {
        int port;
        try (ServerSocket ephemeral = new ServerSocket(0)) {
            port = ephemeral.getLocalPort();
        }
        java.nio.file.Path dataDir = null;
        BrokerServices services;
        if (durable) {
            dataDir = Files.createTempDirectory("javastream-bench-");
            Properties props = ConfigLoader.loadPropertiesForStorage();
            props.setProperty("storage.flush.policy", flushPolicy.name());
            props.setProperty("storage.flush.interval.ms", "60000");
            JavaStreamConfig config =
                    new JavaStreamConfig("benchmark", 0, "127.0.0.1", port, dataDir.toString(), false, 1);
            StorageSettings storageSettings = StorageSettings.from(config, props);
            services = new BrokerServices(new FileMessageStore(storageSettings), 1, 0);
        } else {
            services = new BrokerServices(new InMemoryMessageStore(), 1);
        }
        BrokerServer server = BrokerServer.forLoadTest("127.0.0.1", port, services);
        server.start();
        return new EmbeddedBenchmarkBroker(server, dataDir);
    }

    public int port() {
        return server.boundPort();
    }

    public BrokerServer server() {
        return server;
    }

    @Override
    public void close() {
        server.stop();
        if (dataDir != null) {
            try {
                Files.walk(dataDir)
                        .sorted(java.util.Comparator.reverseOrder())
                        .forEach(
                                path -> {
                                    try {
                                        Files.deleteIfExists(path);
                                    } catch (IOException ignored) {
                                        // best-effort cleanup
                                    }
                                });
            } catch (IOException ignored) {
                // best-effort cleanup
            }
        }
    }
}
