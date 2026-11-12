package com.javastream.protocol;

import com.javastream.broker.BrokerServices;
import com.javastream.client.TcpProtocolConnection;
import com.javastream.config.JavaStreamConfig;
import com.javastream.security.SecuritySettings;
import com.javastream.server.BrokerServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Ensures legacy JavaStream v1 clients work when security features are disabled (default). */
class ProtocolCompatibilityTest {

  private BrokerServer server;

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  void legacyClientCanPingListAndProduceWithoutAuth(@TempDir Path dataDir) throws Exception {
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
    try (TcpProtocolConnection client = new TcpProtocolConnection("127.0.0.1", server.boundPort())) {
      client.connect();
      assertEquals(StatusCode.OK, client.send(OperationCode.PING, new byte[0]).status());
      assertEquals(StatusCode.OK, client.send(OperationCode.LIST_TOPICS, new byte[0]).status());
      client.send(
              OperationCode.CREATE_TOPIC,
              RecordPayloadCodec.encodeCreateTopicRequest(new RecordPayloadCodec.CreateTopicRequest("compat", 1)));
      assertEquals(
              StatusCode.OK,
              client
                      .send(
                              OperationCode.PRODUCE,
                              RecordPayloadCodec.encodeProduceRequest(
                                      new RecordPayloadCodec.ProduceRequest(
                                              "compat",
                                              RecordPayloadCodec.AUTO_PARTITION,
                                              null,
                                              "v1".getBytes(),
                                              java.util.Map.of())))
                      .status());
    }
  }
}
