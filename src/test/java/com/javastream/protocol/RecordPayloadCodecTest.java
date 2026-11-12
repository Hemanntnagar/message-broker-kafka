package com.javastream.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RecordPayloadCodecTest {

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
