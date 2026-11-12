package com.javastream.server;

import com.javastream.protocol.OperationCode;
import com.javastream.protocol.ProtocolConstants;
import com.javastream.protocol.ProtocolDecoder;
import com.javastream.protocol.ProtocolEncoder;
import com.javastream.protocol.Request;
import com.javastream.protocol.Response;
import com.javastream.protocol.StatusCode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

  private static final int MAX_FRAME = 65_536;

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
    server = new BrokerServer("127.0.0.1", freePort, 5_000, MAX_FRAME, 8);

    server.start();
    assertTrue(server.isRunning());

    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
      OutputStream out = socket.getOutputStream();
      InputStream in = socket.getInputStream();

      ProtocolEncoder encoder = new ProtocolEncoder(MAX_FRAME);
      int correlationId = 77;
      out.write(
          encoder.encodeRequest(
              new Request(ProtocolConstants.VERSION_1, correlationId, OperationCode.PING, new byte[0])));
      out.flush();

      Response response = readResponse(in);
      assertEquals(correlationId, response.correlationId());
      assertEquals(StatusCode.OK, response.status());
    }
  }

  @Test
  void multipleConcurrentClients() throws Exception {
    int freePort = reserveEphemeralPort();
    server = new BrokerServer("127.0.0.1", freePort, 5_000, MAX_FRAME, 32);
    server.start();

    int clients = 8;
    CountDownLatch done = new CountDownLatch(clients);
    AtomicInteger successes = new AtomicInteger();

    try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < clients; i++) {
        final int correlationId = i + 1;
        pool.submit(
            () -> {
              try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                ProtocolEncoder encoder = new ProtocolEncoder(MAX_FRAME);
                out.write(
                    encoder.encodeRequest(
                        new Request(
                            ProtocolConstants.VERSION_1, correlationId, OperationCode.PING, new byte[0])));
                out.flush();
                Response response = readResponse(in);
                assertEquals(correlationId, response.correlationId());
                assertEquals(StatusCode.OK, response.status());
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
    server = new BrokerServer("127.0.0.1", freePort, 2_000, MAX_FRAME, 8);
    server.start();

    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
    }

    assertTrue(server.isRunning());

    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
      ProtocolEncoder encoder = new ProtocolEncoder(MAX_FRAME);
      socket.getOutputStream().write(
          encoder.encodeRequest(
              new Request(ProtocolConstants.VERSION_1, 9, OperationCode.LIST_TOPICS, new byte[0])));
      socket.getOutputStream().flush();
      assertEquals(StatusCode.OK, readResponse(socket.getInputStream()).status());
    }
  }

  @Test
  void malformedRequestClosesConnectionWithoutStoppingServer() throws Exception {
    int freePort = reserveEphemeralPort();
    server = new BrokerServer("127.0.0.1", freePort, 2_000, 256, 8);
    server.start();

    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
      socket.getOutputStream().write(new byte[] {0x00, 0x01, 0x02, 0x03});
      socket.getOutputStream().flush();
    }

    Thread.sleep(100);
    assertTrue(server.isRunning());

    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
      ProtocolEncoder encoder = new ProtocolEncoder(256);
      socket.getOutputStream()
          .write(
              encoder.encodeRequest(
                  new Request(ProtocolConstants.VERSION_1, 1, OperationCode.PING, new byte[0])));
      socket.getOutputStream().flush();
      assertEquals(StatusCode.OK, readResponse(socket.getInputStream()).status());
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

  private static Response readResponse(InputStream in) throws IOException {
    byte[] header = in.readNBytes(ProtocolConstants.RESPONSE_HEADER_BYTES);
    ByteBuffer view = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
    view.getInt();
    view.getShort();
    view.getInt();
    view.getShort();
    int payloadLength = view.getInt();
    byte[] payload = payloadLength > 0 ? in.readNBytes(payloadLength) : new byte[0];
    byte[] frame = new byte[header.length + payload.length];
    System.arraycopy(header, 0, frame, 0, header.length);
    System.arraycopy(payload, 0, frame, header.length, payload.length);
    return ProtocolDecoder.decodeResponse(ByteBuffer.wrap(frame), ProtocolConstants.VERSION_1, MAX_FRAME);
  }

  private static int reserveEphemeralPort() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }
}
