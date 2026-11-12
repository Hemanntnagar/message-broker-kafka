package com.javastream.server;

import com.javastream.protocol.OperationCode;
import com.javastream.protocol.ProtocolConstants;
import com.javastream.protocol.ProtocolDecoder;
import com.javastream.protocol.ProtocolEncoder;
import com.javastream.protocol.Request;
import com.javastream.protocol.Response;
import com.javastream.protocol.StatusCode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtocolTcpIntegrationTest {

    private static final int MAX_FRAME = 65_536;

    private BrokerServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void pingOverTcpReturnsOkWithMatchingCorrelationId() throws Exception {
        server = startServerOnEphemeralPort();
        ProtocolEncoder encoder = new ProtocolEncoder(MAX_FRAME);

        try (Socket socket = connect(server)) {
            OutputStream out = socket.getOutputStream();
            Request ping = new Request(ProtocolConstants.VERSION_1, 9001, OperationCode.PING, new byte[] {1, 2});
            out.write(encoder.encodeRequest(ping));
            out.flush();

            Response response = readOneResponse(socket.getInputStream(), MAX_FRAME);
            assertEquals(9001, response.correlationId());
            assertEquals(StatusCode.OK, response.status());
            assertEquals(0, response.payloadLength());
        }
    }

    @Test
    void twoRequestsInOneTcpWriteReceiveTwoResponses() throws Exception {
        server = startServerOnEphemeralPort();
        ProtocolEncoder encoder = new ProtocolEncoder(MAX_FRAME);

        try (Socket socket = connect(server)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            byte[] batch =
                    concat(
                            encoder.encodeRequest(
                                    new Request(ProtocolConstants.VERSION_1, 1, OperationCode.PING, new byte[0])),
                            encoder.encodeRequest(
                                    new Request(
                                            ProtocolConstants.VERSION_1, 2, OperationCode.GET_METADATA, new byte[0])));
            out.write(batch);
            out.flush();

            Response first = readOneResponse(in, MAX_FRAME);
            Response second = readOneResponse(in, MAX_FRAME);
            assertEquals(1, first.correlationId());
            assertEquals(2, second.correlationId());
            assertEquals(StatusCode.OK, first.status());
            assertEquals(StatusCode.OK, second.status());
        }
    }

    @Test
    void malformedMagicClosesConnectionWithoutStoppingServer() throws Exception {
        server = startServerOnEphemeralPort();
        int port = server.boundPort();
        ProtocolEncoder encoder = new ProtocolEncoder(MAX_FRAME);

        try (Socket bad = new Socket()) {
            bad.connect(new InetSocketAddress("127.0.0.1", port), 3_000);
            bad.getOutputStream().write(new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05});
            bad.getOutputStream().flush();
        }

        Thread.sleep(100);
        assertTrue(server.isRunning());

        try (Socket socket = connect(server)) {
            byte[] wire =
                    encoder.encodeRequest(new Request(ProtocolConstants.VERSION_1, 5, OperationCode.PING, new byte[0]));
            socket.getOutputStream().write(wire);
            socket.getOutputStream().flush();
            Response response = readOneResponse(socket.getInputStream(), MAX_FRAME);
            assertEquals(5, response.correlationId());
        }
    }

    private static BrokerServer startServerOnEphemeralPort() throws IOException {
        try (ServerSocket ephemeral = new ServerSocket(0)) {
            int port = ephemeral.getLocalPort();
            BrokerServer broker = new BrokerServer("127.0.0.1", port, 5_000, MAX_FRAME, 8);
            broker.start();
            return broker;
        }
    }

    private static Socket connect(BrokerServer server) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 3_000);
        return socket;
    }

    private static Response readOneResponse(InputStream in, int maxPayloadBytes) throws IOException {
        byte[] header = in.readNBytes(ProtocolConstants.RESPONSE_HEADER_BYTES);
        if (header.length < ProtocolConstants.RESPONSE_HEADER_BYTES) {
            throw new IOException("Truncated response header");
        }
        ByteBuffer headerView = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
        headerView.getInt();
        headerView.getShort();
        headerView.getInt();
        headerView.getShort();
        int payloadLength = headerView.getInt();
        byte[] payload = payloadLength > 0 ? in.readNBytes(payloadLength) : new byte[0];
        if (payload.length != payloadLength) {
            throw new IOException("Truncated response payload");
        }
        byte[] frame = new byte[header.length + payload.length];
        System.arraycopy(header, 0, frame, 0, header.length);
        System.arraycopy(payload, 0, frame, header.length, payload.length);
        return ProtocolDecoder.decodeResponse(
                ByteBuffer.wrap(frame), ProtocolConstants.VERSION_1, maxPayloadBytes);
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
