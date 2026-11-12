package com.javastream.server;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerServerTest {

    private BrokerServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void roundTripFramedRequestOnEphemeralPort() throws Exception {
        int freePort = reserveEphemeralPort();
        server = new BrokerServer("127.0.0.1", freePort, 5_000, 65_536, 8);

        server.start();
        assertTrue(server.isRunning());

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            byte[] request = "hello-phase1".getBytes();
            out.writeInt(request.length);
            out.write(request);
            out.flush();

            int responseLength = in.readInt();
            assertEquals(0, responseLength);
        }
    }

    @Test
    void multipleConcurrentClients() throws Exception {
        int freePort = reserveEphemeralPort();
        server = new BrokerServer("127.0.0.1", freePort, 5_000, 65_536, 32);
        server.start();

        int clients = 8;
        CountDownLatch done = new CountDownLatch(clients);
        AtomicInteger successes = new AtomicInteger();

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < clients; i++) {
                pool.submit(() -> {
                    try (Socket socket = new Socket()) {
                        socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        byte[] payload = new byte[] {1, 2, 3};
                        out.writeInt(payload.length);
                        out.write(payload);
                        out.flush();
                        assertEquals(0, in.readInt());
                        successes.incrementAndGet();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(10, TimeUnit.SECONDS));
        }

        assertEquals(clients, successes.get());
        assertTrue(server.isRunning());
    }

    @Test
    void unexpectedDisconnectDoesNotStopServer() throws Exception {
        int freePort = reserveEphemeralPort();
        server = new BrokerServer("127.0.0.1", freePort, 2_000, 65_536, 8);
        server.start();

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
        }

        assertTrue(server.isRunning());

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(4);
            out.write(new byte[] {9, 9, 9, 9});
            out.flush();
            DataInputStream in = new DataInputStream(socket.getInputStream());
            assertEquals(0, in.readInt());
        }
    }

    @Test
    void malformedRequestLengthClosesConnectionWithoutStoppingServer() throws Exception {
        int freePort = reserveEphemeralPort();
        server = new BrokerServer("127.0.0.1", freePort, 2_000, 256, 8);
        server.start();

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(-1);
            out.flush();
        }

        Thread.sleep(100);
        assertTrue(server.isRunning());

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            out.writeInt(1);
            out.write(42);
            out.flush();
            assertEquals(0, in.readInt());
        }
    }

    @Test
    void gracefulShutdownReleasesPort() throws Exception {
        int freePort = reserveEphemeralPort();
        server = new BrokerServer("127.0.0.1", freePort, 2_000, 256, 8);
        server.start();

        List<Socket> clients = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
            clients.add(socket);
        }

        server.stop();
        assertFalse(server.isRunning());

        for (Socket socket : clients) {
            socket.close();
        }

        try (ServerSocket rebound = new ServerSocket()) {
            rebound.setReuseAddress(true);
            rebound.bind(new InetSocketAddress("127.0.0.1", freePort));
        }
    }

    private static int reserveEphemeralPort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
