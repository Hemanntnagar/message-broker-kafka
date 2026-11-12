package com.javastream.security;

import com.javastream.broker.BrokerServices;
import com.javastream.client.TcpProtocolConnection;
import com.javastream.config.JavaStreamConfig;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.StatusCode;
import com.javastream.server.BrokerServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecurityIntegrationTest {

  private BrokerServer server;

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  void unauthenticatedProduceRejectedWhenClientAuthEnabled(@TempDir Path dataDir) throws Exception {
    String hash = PasswordHasher.hashPassword("pw".toCharArray());
    SecuritySettings security =
        new SecuritySettings(
                true,
                new ClientAuthenticator(
                        true,
                        UserCredentialStore.ofHashed(java.util.Map.of("producer", hash))),
                TopicAccessControl.of(
                        true,
                        java.util.List.of(
                                new AclRule(
                                        "producer",
                                        "secure-topic",
                                        java.util.Set.of(TopicPermission.PRODUCE, TopicPermission.ADMIN)))),
                64 * 1024,
                64 * 1024,
                32,
                TlsSettings.disabled());
    JavaStreamConfig config = securedConfig(dataDir, security, 0);
    server = new BrokerServer(config, new BrokerServices(config));
    server.start();

    try (TcpProtocolConnection client = new TcpProtocolConnection("127.0.0.1", server.boundPort())) {
      client.connect();
      var response =
          client.sendUnchecked(
                  OperationCode.PRODUCE,
                  RecordPayloadCodec.encodeProduceRequest(
                          new RecordPayloadCodec.ProduceRequest(
                                  "secure-topic",
                                  RecordPayloadCodec.AUTO_PARTITION,
                                  null,
                                  "hello".getBytes(),
                                  java.util.Map.of())));
      assertEquals(StatusCode.FORBIDDEN, response.status());
      assertTrue(client.authenticate("producer", "pw"));
      client.send(
              OperationCode.CREATE_TOPIC,
              RecordPayloadCodec.encodeCreateTopicRequest(
                      new RecordPayloadCodec.CreateTopicRequest("secure-topic", 1)));
      var ok =
          client.send(
                  OperationCode.PRODUCE,
                  RecordPayloadCodec.encodeProduceRequest(
                          new RecordPayloadCodec.ProduceRequest(
                                  "secure-topic",
                                  RecordPayloadCodec.AUTO_PARTITION,
                                  null,
                                  "hello".getBytes(),
                                  java.util.Map.of())));
      assertEquals(StatusCode.OK, ok.status());
    }
  }

  @Test
  void oversizedProduceReturnsPayloadTooLarge(@TempDir Path dataDir) throws Exception {
    SecuritySettings security =
        new SecuritySettings(
                false,
                new ClientAuthenticator(false, UserCredentialStore.empty()),
                TopicAccessControl.disabled(),
                16,
                64 * 1024,
                32,
                TlsSettings.disabled());
    JavaStreamConfig config = securedConfig(dataDir, security, 0);
    server = new BrokerServer(config, new BrokerServices(config));
    server.start();

    try (TcpProtocolConnection client = new TcpProtocolConnection("127.0.0.1", server.boundPort())) {
      client.connect();
      byte[] big = new byte[32];
      var create =
          client.send(
                  OperationCode.CREATE_TOPIC,
                  RecordPayloadCodec.encodeCreateTopicRequest(
                          new RecordPayloadCodec.CreateTopicRequest("big", 1)));
      assertEquals(StatusCode.OK, create.status());
      var produce =
          client.sendUnchecked(
                  OperationCode.PRODUCE,
                  RecordPayloadCodec.encodeProduceRequest(
                          new RecordPayloadCodec.ProduceRequest(
                                  "big", RecordPayloadCodec.AUTO_PARTITION, null, big, java.util.Map.of())));
      assertEquals(StatusCode.PAYLOAD_TOO_LARGE, produce.status());
    }
  }

  @Test
  void aclDeniesCrossTopicAccess(@TempDir Path dataDir) throws Exception {
    SecuritySettings security =
        new SecuritySettings(
                false,
                new ClientAuthenticator(false, UserCredentialStore.empty()),
                TopicAccessControl.of(
                        true,
                        java.util.List.of(
                                new AclRule(
                                        "anonymous",
                                        "allowed",
                                        java.util.Set.of(TopicPermission.ADMIN, TopicPermission.PRODUCE)),
                                new AclRule("anonymous", "denied", java.util.Set.of(TopicPermission.ADMIN)))),
                64 * 1024,
                64 * 1024,
                32,
                TlsSettings.disabled());
    JavaStreamConfig config = securedConfig(dataDir, security, 0);
    server = new BrokerServer(config, new BrokerServices(config));
    server.start();

    try (TcpProtocolConnection client = new TcpProtocolConnection("127.0.0.1", server.boundPort())) {
      client.connect();
      client.send(
              OperationCode.CREATE_TOPIC,
              RecordPayloadCodec.encodeCreateTopicRequest(
                      new RecordPayloadCodec.CreateTopicRequest("allowed", 1)));
      client.send(
              OperationCode.CREATE_TOPIC,
              RecordPayloadCodec.encodeCreateTopicRequest(
                      new RecordPayloadCodec.CreateTopicRequest("denied", 1)));
      var forbidden =
          client.sendUnchecked(
                  OperationCode.PRODUCE,
                  RecordPayloadCodec.encodeProduceRequest(
                          new RecordPayloadCodec.ProduceRequest(
                                  "denied",
                                  RecordPayloadCodec.AUTO_PARTITION,
                                  null,
                                  "x".getBytes(),
                                  java.util.Map.of())));
      assertEquals(StatusCode.FORBIDDEN, forbidden.status());
    }
  }

  @Test
  void invalidCredentialsRejected(@TempDir Path dataDir) throws Exception {
    String hash = PasswordHasher.hashPassword("good".toCharArray());
    SecuritySettings security =
        new SecuritySettings(
                true,
                new ClientAuthenticator(true, UserCredentialStore.ofHashed(java.util.Map.of("u", hash))),
                TopicAccessControl.disabled(),
                64 * 1024,
                64 * 1024,
                32,
                TlsSettings.disabled());
    server = new BrokerServer(securedConfig(dataDir, security, 0), new BrokerServices(securedConfig(dataDir, security, 0)));
    server.start();
    try (TcpProtocolConnection client = new TcpProtocolConnection("127.0.0.1", server.boundPort())) {
      client.connect();
      assertFalse(client.authenticate("u", "bad"));
    }
  }

  private static JavaStreamConfig securedConfig(Path dataDir, SecuritySettings security, int port) {
    return new JavaStreamConfig(
            "test",
            0,
            "127.0.0.1",
            port,
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
            security);
  }
}
