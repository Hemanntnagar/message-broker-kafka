package com.javastream.protocol;

import com.javastream.exception.ProtocolDecodeException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtocolEncoderDecoderTest {

  private final ProtocolEncoder encoder = new ProtocolEncoder(65_536);
  private final ProtocolDecoder decoder = new ProtocolDecoder(65_536);

  @Test
  void requestRoundTrip() {
    byte[] payload = new byte[] {10, 20, 30};
    Request original =
        new Request(ProtocolConstants.VERSION_1, 42, OperationCode.PRODUCE, payload);

    byte[] wire = encoder.encodeRequest(original);
    decoder.feed(wire);

    List<Request> decoded = decoder.decodeAvailable();
    assertEquals(1, decoded.size());
    assertEquals(original, decoded.getFirst());
  }

  @Test
  void responseRoundTrip() {
    Response original = new Response(ProtocolConstants.VERSION_1, 7, StatusCode.OK, new byte[] {1});

    byte[] wire = encoder.encodeResponse(original);
    Response decoded =
        ProtocolDecoder.decodeResponse(ByteBuffer.wrap(wire), ProtocolConstants.VERSION_1, 65_536);

    assertEquals(original, decoded);
  }

  @Test
  void partialReadThenCompletion() {
    Request original = new Request(ProtocolConstants.VERSION_1, 1, OperationCode.PING, new byte[0]);
    byte[] wire = encoder.encodeRequest(original);

    decoder.feed(wire, 0, 10);
    assertTrue(decoder.decodeAvailable().isEmpty());
    assertEquals(10, decoder.bufferedByteCount());

    decoder.feed(wire, 10, wire.length - 10);
    List<Request> decoded = decoder.decodeAvailable();
    assertEquals(1, decoded.size());
    assertEquals(original, decoded.getFirst());
    assertEquals(0, decoder.bufferedByteCount());
  }

  @Test
  void multipleFramesInOneRead() {
    Request first = new Request(ProtocolConstants.VERSION_1, 1, OperationCode.PING, new byte[0]);
    Request second =
        new Request(ProtocolConstants.VERSION_1, 2, OperationCode.LIST_TOPICS, "ab".getBytes());

    byte[] combined = concat(encoder.encodeRequest(first), encoder.encodeRequest(second));
    decoder.feed(combined);

    List<Request> decoded = decoder.decodeAvailable();
    assertEquals(2, decoded.size());
    assertEquals(first, decoded.get(0));
    assertEquals(second, decoded.get(1));
  }

  @Test
  void rejectsInvalidMagic() {
    byte[] wire = encoder.encodeRequest(new Request(ProtocolConstants.VERSION_1, 1, OperationCode.PING, new byte[0]));
    wire[0] = (byte) 0xFF;
    decoder.feed(wire);
    assertThrows(ProtocolDecodeException.class, decoder::decodeAvailable);
  }

  @Test
  void rejectsUnsupportedVersion() {
    Request wrongVersion = new Request((short) 99, 1, OperationCode.PING, new byte[0]);
    decoder.feed(encoder.encodeRequest(wrongVersion));
    assertThrows(ProtocolDecodeException.class, decoder::decodeAvailable);
  }

  @Test
  void rejectsOversizedDeclaredPayload() {
    byte[] wire = encoder.encodeRequest(new Request(ProtocolConstants.VERSION_1, 1, OperationCode.PING, new byte[0]));
    ByteBuffer.wrap(wire).order(ByteOrder.BIG_ENDIAN).putInt(12, 200_000);
    ProtocolDecoder smallLimit = new ProtocolDecoder(1024);
    smallLimit.feed(wire);
    assertThrows(ProtocolDecodeException.class, smallLimit::decodeAvailable);
  }

  @Test
  void rejectsNegativePayloadLength() {
    byte[] wire = encoder.encodeRequest(new Request(ProtocolConstants.VERSION_1, 1, OperationCode.PING, new byte[0]));
    ByteBuffer.wrap(wire).order(ByteOrder.BIG_ENDIAN).putInt(12, -1);
    decoder.feed(wire);
    assertThrows(ProtocolDecodeException.class, decoder::decodeAvailable);
  }

  @Test
  void encoderRejectsOversizedPayload() {
    ProtocolEncoder capped = new ProtocolEncoder(4);
    assertThrows(
        com.javastream.exception.JavaStreamException.class,
        () -> capped.encodeRequest(new Request(ProtocolConstants.VERSION_1, 1, OperationCode.PING, new byte[8])));
  }

  @Test
  void pingPayloadEchoRoundTrip() {
    byte[] payload = "ping-body".getBytes();
    Request req = new Request(ProtocolConstants.VERSION_1, 100, OperationCode.PING, payload);
    byte[] wire = encoder.encodeRequest(req);
    decoder.reset();
    decoder.feed(wire);
    Request decoded = decoder.decodeAvailable().getFirst();
    assertArrayEquals(payload, decoded.payload());
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] out = new byte[a.length + b.length];
    System.arraycopy(a, 0, out, 0, a.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }
}
