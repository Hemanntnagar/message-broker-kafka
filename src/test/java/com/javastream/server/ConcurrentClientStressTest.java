package com.javastream.server;

import com.javastream.broker.BrokerServices;
import com.javastream.client.TcpProtocolConnection;
import com.javastream.config.JavaStreamConfig;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.security.SecuritySettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrentClientStressTest {

  private BrokerServer server;

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  void concurrentProducersPublishWithoutLostAcks(@TempDir Path dataDir) throws Exception {
    JavaStreamConfig config =
            new JavaStreamConfig(
                    "test",
                    0,
                    "127.0.0.1",
                    0,
                    dataDir.toString(),
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
                    "",
                    SecuritySettings.disabled());
    server = new BrokerServer(config, new BrokerServices(config));
    server.start();

    try (TcpProtocolConnection setup = new TcpProtocolConnection("127.0.0.1", server.boundPort())) {
      setup.connect();
      setup.send(
              OperationCode.CREATE_TOPIC,
              RecordPayloadCodec.encodeCreateTopicRequest(
                      new RecordPayloadCodec.CreateTopicRequest("stress", 4)));
    }

    int clients = 16;
    int messagesPerClient = 25;
    try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Callable<Integer>> tasks = new ArrayList<>();
      for (int c = 0; c < clients; c++) {
        final int clientId = c;
        tasks.add(
                () -> {
                  try (TcpProtocolConnection conn = new TcpProtocolConnection("127.0.0.1", server.boundPort())) {
                    conn.connect();
                    int ok = 0;
                    for (int i = 0; i < messagesPerClient; i++) {
                      conn.send(
                              OperationCode.PRODUCE,
                              RecordPayloadCodec.encodeProduceRequest(
                                      new RecordPayloadCodec.ProduceRequest(
                                              "stress",
                                              RecordPayloadCodec.AUTO_PARTITION,
                                              ("k" + clientId).getBytes(),
                                              ("v" + i).getBytes(),
                                              java.util.Map.of())));
                      ok++;
                    }
                    return ok;
                  }
                });
      }
      List<Future<Integer>> futures = pool.invokeAll(tasks);
      int total = 0;
      for (Future<Integer> future : futures) {
        total += future.get();
      }
      assertEquals(clients * messagesPerClient, total);
    }
    assertTrue(server.activeConnectionCount() <= 2);
  }
}
