package com.javastream.client;

import com.javastream.exception.BrokerResponseException;
import com.javastream.exception.ClientNotConnectedException;
import com.javastream.exception.JavaStreamException;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.ProtocolConstants;
import com.javastream.protocol.ProtocolDecoder;
import com.javastream.protocol.ProtocolEncoder;
import com.javastream.protocol.Request;
import com.javastream.protocol.Response;
import com.javastream.protocol.StatusCode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Synchronous TCP client for JavaStream v1 request/response pairs on a single connection.
 */
public class TcpProtocolConnection implements AutoCloseable {

    private final String host;
    private final int port;
    private final int connectTimeoutMillis;
    private final int socketTimeoutMillis;
    private final int maxPayloadBytes;

    private final AtomicInteger correlationId = new AtomicInteger(1);
    private final ProtocolEncoder encoder;
    private volatile Socket socket;
    private volatile InputStream input;
    private volatile OutputStream output;

    public TcpProtocolConnection(String host, int port) {
        this(host, port, 5_000, 30_000, 1_048_576);
    }

    public TcpProtocolConnection(
            String host, int port, int connectTimeoutMillis, int socketTimeoutMillis, int maxPayloadBytes) {
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.socketTimeoutMillis = socketTimeoutMillis;
        this.maxPayloadBytes = maxPayloadBytes;
        this.encoder = new ProtocolEncoder(maxPayloadBytes);
    }

    public synchronized void connect() throws IOException {
        if (socket != null && !socket.isClosed()) {
            return;
        }
        Socket connected = new Socket();
        connected.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
        connected.setSoTimeout(socketTimeoutMillis);
        connected.setTcpNoDelay(true);
        socket = connected;
        input = connected.getInputStream();
        output = connected.getOutputStream();
    }

    public boolean isConnected() {
        Socket current = socket;
        return current != null && current.isConnected() && !current.isClosed();
    }

    public synchronized Response send(OperationCode operation, byte[] payload) throws IOException {
        return send(operation, payload, null);
    }

    public synchronized Response send(OperationCode operation, byte[] payload, Duration requestTimeout)
            throws IOException {
        if (!isConnected()) {
            throw new ClientNotConnectedException("Client is not connected to %s:%d".formatted(host, port));
        }
        int id = correlationId.getAndIncrement();
        Request request = new Request(ProtocolConstants.VERSION_1, id, operation, payload == null ? new byte[0] : payload);
        int previousTimeout = socketTimeoutMillis;
        Socket current = socket;
        if (requestTimeout != null && current != null) {
            int millis = Math.toIntExact(Math.min(Integer.MAX_VALUE, requestTimeout.toMillis()));
            current.setSoTimeout(Math.max(1, millis));
        }
        try {
            output.write(encoder.encodeRequest(request));
            output.flush();
            Response response = readResponse();
            if (response.correlationId() != id) {
                throw new JavaStreamException(
                        "Correlation mismatch: expected %d got %d".formatted(id, response.correlationId()));
            }
            if (response.status() != StatusCode.OK) {
                throw new BrokerResponseException(
                        response.status(),
                        "Broker returned %s for operation %s".formatted(response.status(), operation));
            }
            return response;
        } catch (IOException e) {
            closeQuietly();
            throw e;
        } finally {
            if (requestTimeout != null && current != null && current.isConnected() && !current.isClosed()) {
                current.setSoTimeout(previousTimeout);
            }
        }
    }

    @Override
    public synchronized void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        Socket current = socket;
        socket = null;
        input = null;
        output = null;
        if (current != null) {
            try {
                current.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
    }

    private Response readResponse() throws IOException {
        byte[] header = input.readNBytes(ProtocolConstants.RESPONSE_HEADER_BYTES);
        if (header.length < ProtocolConstants.RESPONSE_HEADER_BYTES) {
            throw new IOException("Broker closed connection while reading response header");
        }
        ByteBuffer headerView = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
        headerView.getInt();
        headerView.getShort();
        headerView.getInt();
        headerView.getShort();
        int payloadLength = headerView.getInt();
        if (payloadLength > maxPayloadBytes) {
            throw new IOException("Response payload too large: " + payloadLength);
        }
        byte[] payload = payloadLength > 0 ? input.readNBytes(payloadLength) : new byte[0];
        if (payload.length != payloadLength) {
            throw new IOException("Broker closed connection while reading response payload");
        }
        byte[] frame = new byte[header.length + payload.length];
        System.arraycopy(header, 0, frame, 0, header.length);
        System.arraycopy(payload, 0, frame, header.length, payload.length);
        return ProtocolDecoder.decodeResponse(
                ByteBuffer.wrap(frame), ProtocolConstants.VERSION_1, maxPayloadBytes);
    }
}
