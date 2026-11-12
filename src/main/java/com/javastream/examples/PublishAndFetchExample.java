package com.javastream.examples;

import com.javastream.client.TcpProtocolConnection;
import com.javastream.delivery.ProducerAckLevel;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.Response;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/** Low-level PRODUCE and FETCH round trip. */
public final class PublishAndFetchExample {

    public static void main(String[] args) throws Exception {
        String host = ExampleBrokerTarget.host();
        int port = ExampleBrokerTarget.port();
        String topic = "example-publish-fetch";

        try (TcpProtocolConnection connection = new TcpProtocolConnection(host, port)) {
            connection.connect();

            connection.send(
                    OperationCode.CREATE_TOPIC,
                    RecordPayloadCodec.encodeCreateTopicRequest(
                            new RecordPayloadCodec.CreateTopicRequest(topic, 1)),
                    Duration.ofSeconds(5));

            RecordPayloadCodec.ProduceRequest produce =
                    new RecordPayloadCodec.ProduceRequest(
                            topic,
                            0,
                            "key".getBytes(StandardCharsets.UTF_8),
                            "payload".getBytes(StandardCharsets.UTF_8),
                            Map.of(),
                            ProducerAckLevel.ACK_LEADER,
                            5_000,
                            -1,
                            0,
                            List.of());
            Response produceResponse =
                    connection.send(
                            OperationCode.PRODUCE,
                            RecordPayloadCodec.encodeProduceRequest(produce),
                            Duration.ofSeconds(10));
            RecordPayloadCodec.ProduceResponse ack =
                    RecordPayloadCodec.decodeProduceResponse(produceResponse.payload()).responses().getFirst();
            System.out.printf("Produced offset=%d partition=%d%n", ack.offset(), ack.partition());

            RecordPayloadCodec.FetchRequest fetch =
                    new RecordPayloadCodec.FetchRequest(topic, ack.partition(), ack.offset(), 10, 2_000);
            Response fetchResponse =
                    connection.send(
                            OperationCode.FETCH,
                            RecordPayloadCodec.encodeFetchRequest(fetch),
                            Duration.ofSeconds(5));
            RecordPayloadCodec.FetchResponse decoded =
                    RecordPayloadCodec.decodeFetchResponse(fetchResponse.payload());
            for (RecordPayloadCodec.FetchedRecord record : decoded.records()) {
                System.out.printf(
                        "Fetched offset=%d value=%s%n",
                        record.offset(), new String(record.value(), StandardCharsets.UTF_8));
            }
        }
    }
}
