package com.javastream.replication.interbroker;

import com.javastream.storage.BrokerMessageStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Synchronous inter-broker RPC client (one request per connection). */
public final class InterBrokerClient implements AutoCloseable {

    private final String host;
    private final int port;
    private final int connectTimeoutMillis;
    private final int socketTimeoutMillis;
    private final AtomicInteger correlationId = new AtomicInteger(1);

    public InterBrokerClient(String host, int port) {
        this(host, port, 5_000, 30_000);
    }

    public InterBrokerClient(String host, int port, int connectTimeoutMillis, int socketTimeoutMillis) {
        this.host = host;
        this.port = port;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.socketTimeoutMillis = socketTimeoutMillis;
    }

    public byte[] rpc(InterBrokerOperation operation, byte[] payload) throws IOException {
        return rpc(operation, payload, Duration.ofMillis(socketTimeoutMillis));
    }

    public byte[] rpc(InterBrokerOperation operation, byte[] payload, Duration timeout) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
            socket.setSoTimeout((int) timeout.toMillis());
            socket.setTcpNoDelay(true);
            int correlation = correlationId.getAndIncrement();
            writeRequest(socket.getOutputStream(), correlation, operation, payload);
            return readResponse(socket.getInputStream(), correlation);
        }
    }

    public InterBrokerCodec.AssignmentPush register(InterBrokerCodec.RegisterRequest request) throws IOException {
        byte[] payload = rpc(InterBrokerOperation.BROKER_REGISTER, InterBrokerCodec.encodeRegister(request));
        return payload.length == 0
                ? new InterBrokerCodec.AssignmentPush(List.of(), List.of())
                : InterBrokerCodec.decodeAssignmentPush(payload);
    }

    public void heartbeat(InterBrokerCodec.HeartbeatRequest request) throws IOException {
        rpc(InterBrokerOperation.BROKER_HEARTBEAT, InterBrokerCodec.encodeHeartbeat(request));
    }

    public void deregister(int brokerId) throws IOException {
        rpc(InterBrokerOperation.BROKER_DEREGISTER, InterBrokerCodec.encodeDeregister(brokerId));
    }

    public InterBrokerCodec.ReplicateRecordResponse replicate(InterBrokerCodec.ReplicateRecordRequest request)
            throws IOException {
        byte[] payload =
                rpc(InterBrokerOperation.REPLICATE_RECORD, InterBrokerCodec.encodeReplicate(request));
        return InterBrokerCodec.decodeReplicateResponse(payload);
    }

    public List<BrokerMessageStore.StoredRecord> replicaFetch(InterBrokerCodec.ReplicaFetchRequest request)
            throws IOException {
        byte[] payload = rpc(InterBrokerOperation.REPLICA_FETCH, InterBrokerCodec.encodeReplicaFetch(request));
        return InterBrokerCodec.decodeReplicaFetchResponse(payload);
    }

    public void notifyTopicCreated(InterBrokerCodec.TopicCreatedNotification notification) throws IOException {
        rpc(InterBrokerOperation.TOPIC_CREATED, InterBrokerCodec.encodeTopicCreated(notification));
    }

    public void pushAssignments(InterBrokerCodec.AssignmentPush push) throws IOException {
        rpc(InterBrokerOperation.ASSIGNMENT_PUSH, InterBrokerCodec.encodeAssignmentPush(push));
    }

    private static void writeRequest(OutputStream output, int correlation, InterBrokerOperation operation, byte[] payload)
            throws IOException {
        ByteBuffer header =
                ByteBuffer.allocate(InterBrokerConstants.HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        header.putInt(InterBrokerConstants.MAGIC);
        header.putShort(InterBrokerConstants.VERSION);
        header.putInt(correlation);
        header.putShort(operation.wire());
        header.putInt(payload.length);
        output.write(header.array());
        if (payload.length > 0) {
            output.write(payload);
        }
        output.flush();
    }

    private static byte[] readResponse(InputStream input, int expectedCorrelation) throws IOException {
        byte[] headerBytes = input.readNBytes(InterBrokerConstants.HEADER_BYTES);
        if (headerBytes.length != InterBrokerConstants.HEADER_BYTES) {
            throw new IOException("Unexpected end of stream while reading inter-broker response header");
        }
        ByteBuffer header = ByteBuffer.wrap(headerBytes).order(ByteOrder.BIG_ENDIAN);
        int magic = header.getInt();
        if (magic != InterBrokerConstants.MAGIC) {
            throw new IOException("Bad inter-broker magic: " + Integer.toHexString(magic));
        }
        short version = header.getShort();
        if (version != InterBrokerConstants.VERSION) {
            throw new IOException("Unsupported inter-broker version: " + version);
        }
        int correlation = header.getInt();
        if (correlation != expectedCorrelation) {
            throw new IOException("Correlation mismatch: expected " + expectedCorrelation + ", got " + correlation);
        }
        short status = header.getShort();
        int length = header.getInt();
        if (length < 0) {
            throw new IOException("Negative inter-broker payload length: " + length);
        }
        byte[] payload = length == 0 ? new byte[0] : input.readNBytes(length);
        if (status != 0) {
            throw new IOException("Inter-broker RPC failed with status " + status);
        }
        return payload;
    }

    @Override
    public void close() {}
}
