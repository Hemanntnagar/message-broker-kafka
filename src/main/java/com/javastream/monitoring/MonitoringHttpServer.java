package com.javastream.monitoring;

import com.javastream.admin.AdminAuthority;
import com.javastream.admin.AdminCommand;
import com.javastream.admin.BrokerAdminService;
import com.javastream.metrics.BrokerMetrics;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/** Lightweight JDK HTTP server exposing health, readiness, metrics, and admin routes. */
public final class MonitoringHttpServer implements AutoCloseable {

    private final String host;
    private final int port;
    private final BrokerMetrics metrics;
    private final BrokerAdminService adminService;
    private final AdminAuthority adminAuthority;
    private final BooleanSupplier ready;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private HttpServer httpServer;

    public MonitoringHttpServer(
            String host,
            int port,
            BrokerMetrics metrics,
            BrokerAdminService adminService,
            AdminAuthority adminAuthority,
            BooleanSupplier ready) {
        this.host = host;
        this.port = port;
        this.metrics = metrics;
        this.adminService = adminService;
        this.adminAuthority = adminAuthority;
        this.ready = ready;
    }

    public void start() {
        if (port <= 0) {
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            httpServer = HttpServer.create(new InetSocketAddress(host, port), 0);
            httpServer.createContext("/health", new JsonHandler(200, () -> "{\"status\":\"UP\"}"));
            httpServer.createContext("/ready", this::handleReady);
            httpServer.createContext("/metrics", this::handleMetrics);
            httpServer.createContext("/admin/", this::handleAdmin);
            httpServer.setExecutor(
                    Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("monitoring-http-", 0).factory()));
            httpServer.start();
        } catch (IOException e) {
            running.set(false);
            throw new UncheckedIOException("Failed to start monitoring HTTP server on " + host + ":" + port, e);
        }
    }

    public int boundPort() {
        return httpServer == null ? -1 : httpServer.getAddress().getPort();
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }

    private void handleReady(HttpExchange exchange) throws IOException {
        if (ready.getAsBoolean()) {
            writeJson(exchange, 200, "{\"status\":\"READY\"}");
        } else {
            writeJson(exchange, 503, "{\"status\":\"NOT_READY\"}");
        }
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        if (!(metrics.meterRegistry() instanceof PrometheusMeterRegistry prometheus)) {
            writeText(exchange, 501, "Prometheus registry not configured\n");
            return;
        }
        writeText(exchange, 200, prometheus.scrape());
    }

    private void handleAdmin(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }
        String token = exchange.getRequestHeaders().getFirst("X-Admin-Token");
        if (!adminAuthority.authorize(token)) {
            writeJson(exchange, 403, "{\"error\":\"forbidden\"}");
            return;
        }
        String path = exchange.getRequestURI().getPath();
        String[] parts = path.substring("/admin/".length()).split("/");
        if (parts.length == 0 || parts[0].isBlank()) {
            writeJson(exchange, 404, "{\"error\":\"not_found\"}");
            return;
        }
        AdminCommand command;
        try {
            command = AdminCommand.valueOf(parts[0].toUpperCase());
        } catch (IllegalArgumentException e) {
            writeJson(exchange, 404, "{\"error\":\"unknown_command\"}");
            return;
        }
        Map<String, String> args = queryArgs(exchange.getRequestURI().getRawQuery());
        try {
            Object result = adminService.execute(command, args);
            writeJson(exchange, 200, AdminJson.encode(result));
        } catch (RuntimeException e) {
            writeJson(exchange, 400, "{\"error\":\"" + escape(e.getMessage()) + "\"}");
        }
    }

    private static Map<String, String> queryArgs(String rawQuery) {
        Map<String, String> args = new HashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return args;
        }
        for (String pair : rawQuery.split("&")) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                args.put(kv[0], kv[1]);
            }
        }
        return args;
    }

    private static void writeJson(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static void writeText(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\"", "\\\"");
    }

    private static final class JsonHandler implements HttpHandler {

        @FunctionalInterface
        private interface BodySupplier {
            String get();
        }

        private final int status;
        private final BodySupplier body;

        private JsonHandler(int status, BodySupplier body) {
            this.status = status;
            this.body = body;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            writeJson(exchange, status, body.get());
        }
    }
}
