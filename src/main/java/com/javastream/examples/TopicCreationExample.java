package com.javastream.examples;

import com.javastream.client.TcpProtocolConnection;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.Response;

import java.time.Duration;

/** Creates a topic and lists catalog entries via the wire protocol. */
public final class TopicCreationExample {

    public static void main(String[] args) throws Exception {
        String host = ExampleBrokerTarget.host();
        int port = ExampleBrokerTarget.port();
        String topic = args.length > 0 ? args[0] : "example-topic";
        int partitionCount = args.length > 1 ? Integer.parseInt(args[1]) : 3;

        try (TcpProtocolConnection connection = new TcpProtocolConnection(host, port)) {
            connection.connect();

            RecordPayloadCodec.CreateTopicRequest create =
                    new RecordPayloadCodec.CreateTopicRequest(topic, partitionCount);
            Response createResponse =
                    connection.send(
                            OperationCode.CREATE_TOPIC,
                            RecordPayloadCodec.encodeCreateTopicRequest(create),
                            Duration.ofSeconds(5));
            System.out.println("CREATE_TOPIC status: " + createResponse.status());

            Response listResponse =
                    connection.send(OperationCode.LIST_TOPICS, new byte[0], Duration.ofSeconds(5));
            RecordPayloadCodec.ListTopicsResponse topics =
                    RecordPayloadCodec.decodeListTopicsResponse(listResponse.payload());
            for (RecordPayloadCodec.TopicEntry entry : topics.topics()) {
                System.out.printf("topic=%s partitions=%d%n", entry.name(), entry.partitionCount());
            }
        }
    }
}
