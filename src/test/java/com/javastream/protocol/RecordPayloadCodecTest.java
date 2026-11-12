package com.javastream.protocol;

import com.javastream.delivery.ProducerAckLevel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RecordPayloadCodecTest {

    @Test
    void legacyProducePayloadWithoutDeliveryTrailer() {
        RecordPayloadCodec.ProduceRequest legacy =
                new RecordPayloadCodec.ProduceRequest(
                        "legacy",
                        0,
                        null,
                        new byte[] {1},
                        Map.of());
        byte[] minimal = encodeLegacyProduce(legacy);
        RecordPayloadCodec.ProduceRequest decoded = RecordPayloadCodec.decodeProduceRequest(minimal);
        assertEquals(ProducerAckLevel.ACK_LEADER, decoded.ackLevel());
        assertEquals(-1, decoded.producerId());
    }

    @Test
    void produceRoundTrip() {
        RecordPayloadCodec.ProduceRequest request =
                new RecordPayloadCodec.ProduceRequest(
                        "topic-a",
                        -1,
                        "key".getBytes(StandardCharsets.UTF_8),
                        "value".getBytes(StandardCharsets.UTF_8),
                        Map.of("h", new byte[] {9}));
        byte[] wire = RecordPayloadCodec.encodeProduceRequest(request);
        RecordPayloadCodec.ProduceRequest decoded = RecordPayloadCodec.decodeProduceRequest(wire);
        assertEquals(request.topic(), decoded.topic());
        assertArrayEquals(request.key(), decoded.key());
        assertArrayEquals(request.value(), decoded.value());
        assertArrayEquals(request.headers().get("h"), decoded.headers().get("h"));
        assertEquals(ProducerAckLevel.ACK_LEADER, decoded.ackLevel());
    }

    private static byte[] encodeLegacyProduce(RecordPayloadCodec.ProduceRequest request) {
        byte[] full = RecordPayloadCodec.encodeProduceRequest(request);
        return java.util.Arrays.copyOf(full, full.length - 21);
    }

    @Test
    void produceBatchRoundTrip() {
        byte[] value = "batch-value".getBytes(StandardCharsets.UTF_8);
        List<RecordPayloadCodec.ProduceRecord> tail =
                List.of(
                        new RecordPayloadCodec.ProduceRecord(null, value, Map.of()),
                        new RecordPayloadCodec.ProduceRecord("k2".getBytes(StandardCharsets.UTF_8), value, Map.of()));
        RecordPayloadCodec.ProduceRequest request =
                new RecordPayloadCodec.ProduceRequest(
                        "batch-topic",
                        0,
                        "k1".getBytes(StandardCharsets.UTF_8),
                        value,
                        Map.of(),
                        ProducerAckLevel.ACK_LEADER,
                        0,
                        -1,
                        0,
                        tail);
        byte[] wire = RecordPayloadCodec.encodeProduceRequest(request);
        RecordPayloadCodec.ProduceRequest decoded = RecordPayloadCodec.decodeProduceRequest(wire);
        assertEquals(3, decoded.allRecords().size());
        RecordPayloadCodec.ProduceBatchResponse response =
                new RecordPayloadCodec.ProduceBatchResponse(
                        List.of(
                                new RecordPayloadCodec.ProduceResponse(1, 0, 0),
                                new RecordPayloadCodec.ProduceResponse(2, 0, 1),
                                new RecordPayloadCodec.ProduceResponse(3, 0, 2)));
        byte[] responseWire = RecordPayloadCodec.encodeProduceBatchResponse(response);
        assertEquals(3, RecordPayloadCodec.decodeProduceResponse(responseWire).responses().size());
    }

    @Test
    void fetchResponseRoundTrip() {
        RecordPayloadCodec.FetchResponse response =
                new RecordPayloadCodec.FetchResponse(
                        List.of(
                                new RecordPayloadCodec.FetchedRecord(
                                        42,
                                        0,
                                        7,
                                        1_700_000_000_000L,
                                        null,
                                        new byte[] {1, 2},
                                        Map.of())));
        byte[] wire = RecordPayloadCodec.encodeFetchResponse(response);
        RecordPayloadCodec.FetchResponse decoded = RecordPayloadCodec.decodeFetchResponse(wire);
        assertEquals(1, decoded.records().size());
        assertEquals(7, decoded.records().getFirst().offset());
    }
}
