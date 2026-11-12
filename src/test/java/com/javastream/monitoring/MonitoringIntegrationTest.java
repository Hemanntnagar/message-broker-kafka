package com.javastream.monitoring;

import com.javastream.admin.AdminAuthority;
import com.javastream.admin.BrokerAdminService;
import com.javastream.broker.BrokerRequestDispatcher;
import com.javastream.broker.BrokerServices;
import com.javastream.metrics.BrokerMetrics;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.ProtocolConstants;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.Request;
import com.javastream.protocol.Response;
import com.javastream.protocol.StatusCode;
import com.javastream.producer.ProducerClient;
import com.javastream.server.BrokerServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MonitoringIntegrationTest {

    private BrokerServer brokerServer;
    private MonitoringHttpServer monitoringServer;

    @AfterEach
    void tearDown() {
        if (brokerServer != null) {
            brokerServer.stop();
        }
        if (monitoringServer != null) {
            monitoringServer.close();
        }
    }

    @Test
    void metricsIncrementOnProduceAndFetch() throws Exception {
        brokerServer = startTestBroker();
        BrokerMetrics metrics = brokerServer.monitoring().metrics();

        try (ProducerClient producer = new ProducerClient("127.0.0.1", brokerServer.boundPort());
                ProducerClient consumer = new ProducerClient("127.0.0.1", brokerServer.boundPort())) {
            producer.connect();
            consumer.connect();
            producer.createTopic("metrics-topic", 1);
            producer.publish(
                    "metrics-topic",
                    "k".getBytes(StandardCharsets.UTF_8),
                    "v".getBytes(StandardCharsets.UTF_8),
                    java.util.Map.of());
        }

        assertTrue(metrics.counterValue("javastream.messages.published") >= 1.0);
        assertTrue(metrics.counterValue("javastream.messages.consumed") >= 0.0);
    }

    @Test
    void healthReadyAndMetricsEndpoints() throws Exception {
        BrokerServices services = new BrokerServices();
        BrokerMetrics metrics =
                new BrokerMetrics(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT), 0, Instant.now());
        metrics.attach(services, Path.of("."), Optional.empty());
        BrokerAdminService admin =
                new BrokerAdminService(services, Optional.empty(), Instant.now(), "127.0.0.1", 9092);
        int monitoringPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            monitoringPort = socket.getLocalPort();
        }
        monitoringServer =
                new MonitoringHttpServer(
                        "127.0.0.1", monitoringPort, metrics, admin, AdminAuthority.disabled(), () -> true);
        monitoringServer.start();

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> health =
                client.send(
                        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + monitoringPort + "/health")).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
        assertEquals(200, health.statusCode());
        assertTrue(health.body().contains("UP"));

        HttpResponse<String> ready =
                client.send(
                        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + monitoringPort + "/ready")).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
        assertEquals(200, ready.statusCode());

        HttpResponse<String> prom =
                client.send(
                        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + monitoringPort + "/metrics")).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
        assertEquals(200, prom.statusCode());
        assertTrue(prom.body().contains("javastream_messages_published"));
    }

    @Test
    void securedCreateTopicRequiresAdminToken() {
        BrokerServices services = new BrokerServices();
        AdminAuthority authority = AdminAuthority.fromConfig("secret");
        BrokerAdminService admin =
                new BrokerAdminService(services, Optional.empty(), Instant.now(), "127.0.0.1", 9092);
        BrokerRequestDispatcher dispatcher =
                new BrokerRequestDispatcher(services, new BrokerMetrics(0), authority, admin);

        byte[] payload =
                RecordPayloadCodec.encodeCreateTopicRequest(
                        new RecordPayloadCodec.CreateTopicRequest("secure-topic", 1));
        Response denied =
                dispatcher.dispatch(
                        new Request(ProtocolConstants.VERSION_1, 1, OperationCode.CREATE_TOPIC, payload));
        assertEquals(StatusCode.FORBIDDEN, denied.status());

        byte[] authorized =
                RecordPayloadCodec.encodeCreateTopicRequest(
                        new RecordPayloadCodec.CreateTopicRequest("secure-topic", 1, Optional.of("secret")));
        Response allowed =
                dispatcher.dispatch(
                        new Request(ProtocolConstants.VERSION_1, 2, OperationCode.CREATE_TOPIC, authorized));
        assertEquals(StatusCode.OK, allowed.status());
    }

    @Test
    void adminHttpRejectsMissingTokenWhenConfigured() throws Exception {
        BrokerServices services = new BrokerServices();
        BrokerMetrics metrics = new BrokerMetrics(0);
        BrokerAdminService admin =
                new BrokerAdminService(services, Optional.empty(), Instant.now(), "127.0.0.1", 9092);
        int monitoringPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            monitoringPort = socket.getLocalPort();
        }
        monitoringServer =
                new MonitoringHttpServer(
                        "127.0.0.1",
                        monitoringPort,
                        metrics,
                        admin,
                        AdminAuthority.fromConfig("secret"),
                        () -> true);
        monitoringServer.start();

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> forbidden =
                client.send(
                        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + monitoringPort + "/admin/LIST_TOPICS"))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertEquals(403, forbidden.statusCode());
    }

    private BrokerServer startTestBroker() throws IOException {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        com.javastream.config.JavaStreamConfig config =
                new com.javastream.config.JavaStreamConfig(
                        "test",
                        0,
                        "127.0.0.1",
                        port,
                        "./data",
                        false,
                        1,
                        0,
                        "127.0.0.1",
                        0,
                        0,
                        1,
                        15_000,
                        2_000,
                        "127.0.0.1",
                        0,
                        "");
        BrokerServer server = new BrokerServer(config, new BrokerServices());
        server.start();
        return server;
    }
}
