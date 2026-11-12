package com.javastream.benchmark.jmh;

import com.javastream.protocol.RecordPayloadCodec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class RecordPayloadCodecBenchmark {

    @State(Scope.Thread)
    public static class PayloadState {
        byte[] singleWire;
        byte[] batchWire;
        RecordPayloadCodec.ProduceRequest single;
        RecordPayloadCodec.ProduceRequest batch;

        @Setup
        public void setup() {
            byte[] value = new byte[256];
            single =
                    new RecordPayloadCodec.ProduceRequest(
                            "bench", 0, "k".getBytes(StandardCharsets.UTF_8), value, Map.of());
            singleWire = RecordPayloadCodec.encodeProduceRequest(single);
            List<RecordPayloadCodec.ProduceRecord> tail = new java.util.ArrayList<>();
            for (int i = 0; i < 9; i++) {
                tail.add(new RecordPayloadCodec.ProduceRecord(single.key(), value, Map.of()));
            }
            batch =
                    new RecordPayloadCodec.ProduceRequest(
                            "bench",
                            0,
                            single.key(),
                            value,
                            Map.of(),
                            com.javastream.delivery.ProducerAckLevel.ACK_LEADER,
                            0,
                            -1,
                            0,
                            tail);
            batchWire = RecordPayloadCodec.encodeProduceRequest(batch);
        }
    }

    @Benchmark
    public byte[] encodeSingle(PayloadState state) {
        return RecordPayloadCodec.encodeProduceRequest(state.single);
    }

    @Benchmark
    public RecordPayloadCodec.ProduceRequest decodeSingle(PayloadState state) {
        return RecordPayloadCodec.decodeProduceRequest(state.singleWire);
    }

    @Benchmark
    public byte[] encodeBatch(PayloadState state) {
        return RecordPayloadCodec.encodeProduceRequest(state.batch);
    }

    @Benchmark
    public RecordPayloadCodec.ProduceRequest decodeBatch(PayloadState state) {
        return RecordPayloadCodec.decodeProduceRequest(state.batchWire);
    }
}
