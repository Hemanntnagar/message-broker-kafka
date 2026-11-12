package com.javastream.protocol;

import com.javastream.exception.ProtocolDecodeException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Incrementally decodes request frames from a TCP byte stream. Supports partial reads and multiple
 * complete frames per {@link #feed} call.
 */
public final class ProtocolDecoder {

  private static final int INITIAL_CAPACITY = 512;

  private final int maxPayloadBytes;
  private final short supportedVersion;
  private final int maxBufferedBytes;

  private ByteBuffer buffer;

  public ProtocolDecoder(int maxPayloadBytes) {
    this(maxPayloadBytes, ProtocolConstants.VERSION_1);
  }

  public ProtocolDecoder(int maxPayloadBytes, short supportedVersion) {
    if (maxPayloadBytes < 0) {
      throw new ProtocolDecodeException("maxPayloadBytes must be non-negative");
    }
    this.maxPayloadBytes = maxPayloadBytes;
    this.supportedVersion = supportedVersion;
    this.maxBufferedBytes = ProtocolConstants.REQUEST_HEADER_BYTES + maxPayloadBytes;
    this.buffer = ByteBuffer.allocate(Math.min(INITIAL_CAPACITY, maxBufferedBytes)).order(ByteOrder.BIG_ENDIAN);
    this.buffer.flip();
  }

  /** Appends bytes from a socket (or test harness) read. */
  public void feed(byte[] chunk) {
    feed(chunk, 0, chunk.length);
  }

  public void feed(byte[] chunk, int offset, int length) {
    if (length == 0) {
      return;
    }
    if (offset < 0 || length < 0 || offset + length > chunk.length) {
      throw new ProtocolDecodeException("Invalid feed window");
    }

    int unconsumed = buffer.remaining();
    if (unconsumed + length > maxBufferedBytes) {
      throw new ProtocolDecodeException(
          "Buffered input exceeds maximum frame size (%d bytes)".formatted(maxBufferedBytes));
    }

    if (buffer.position() > 0 || unconsumed > 0) {
      buffer.compact();
    } else {
      buffer.clear();
    }

    if (buffer.remaining() < length) {
      growBuffer(buffer.position() + length);
    }

    buffer.put(chunk, offset, length);
    buffer.flip();
  }

  /** Returns all fully received request frames, leaving a partial tail in the internal buffer. */
  public List<Request> decodeAvailable() {
    List<Request> decoded = new ArrayList<>();
    while (true) {
      Optional<Request> next = tryDecodeOne();
      if (next.isEmpty()) {
        break;
      }
      decoded.add(next.get());
    }
    return decoded;
  }

  public void reset() {
    buffer = ByteBuffer.allocate(Math.min(INITIAL_CAPACITY, maxBufferedBytes)).order(ByteOrder.BIG_ENDIAN);
    buffer.flip();
  }

  int bufferedByteCount() {
    return buffer.remaining();
  }

  private Optional<Request> tryDecodeOne() {
    if (buffer.remaining() < ProtocolConstants.REQUEST_HEADER_BYTES) {
      return Optional.empty();
    }

    buffer.mark();
    int magic = buffer.getInt();
    if (magic != ProtocolConstants.MAGIC) {
      throw new ProtocolDecodeException(
          "Invalid protocol magic 0x%08X (expected 0x%08X)".formatted(magic, ProtocolConstants.MAGIC));
    }

    short version = buffer.getShort();
    if (version != supportedVersion) {
      throw new ProtocolDecodeException(
          "Unsupported protocol version %d (supported %d)".formatted(version, supportedVersion));
    }

    int correlationId = buffer.getInt();
    short operationWire = buffer.getShort();
    int payloadLength = buffer.getInt();

    if (payloadLength < 0) {
      throw new ProtocolDecodeException("Negative payload length: " + payloadLength);
    }
    if (payloadLength > maxPayloadBytes) {
      throw new ProtocolDecodeException(
          "Payload length %d exceeds maximum %d".formatted(payloadLength, maxPayloadBytes));
    }

    if (buffer.remaining() < payloadLength) {
      buffer.reset();
      return Optional.empty();
    }

    OperationCode operation = OperationCode.fromWire(operationWire);
    byte[] payload = new byte[payloadLength];
    buffer.get(payload);

    return Optional.of(new Request(version, correlationId, operation, payload));
  }

  private void growBuffer(int minimumCapacity) {
    int newCapacity = Math.max(buffer.capacity() * 2, minimumCapacity);
    newCapacity = Math.min(newCapacity, maxBufferedBytes);
    if (newCapacity < minimumCapacity) {
      throw new ProtocolDecodeException(
          "Frame requires %d bytes but maximum buffered frame is %d"
              .formatted(minimumCapacity, maxBufferedBytes));
    }
    ByteBuffer grown = ByteBuffer.allocate(newCapacity).order(ByteOrder.BIG_ENDIAN);
    buffer.flip();
    grown.put(buffer);
    buffer = grown;
  }

  /** Decodes a single complete response frame from a {@link ByteBuffer} (no streaming state). */
  public static Response decodeResponse(ByteBuffer buffer, short supportedVersion, int maxPayloadBytes) {
    buffer.order(ByteOrder.BIG_ENDIAN);
    if (buffer.remaining() < ProtocolConstants.RESPONSE_HEADER_BYTES) {
      throw new ProtocolDecodeException("Response frame header truncated");
    }

    int magic = buffer.getInt();
    if (magic != ProtocolConstants.MAGIC) {
      throw new ProtocolDecodeException(
          "Invalid protocol magic 0x%08X (expected 0x%08X)".formatted(magic, ProtocolConstants.MAGIC));
    }

    short version = buffer.getShort();
    if (version != supportedVersion) {
      throw new ProtocolDecodeException(
          "Unsupported protocol version %d (supported %d)".formatted(version, supportedVersion));
    }

    int correlationId = buffer.getInt();
    short statusWire = buffer.getShort();
    int payloadLength = buffer.getInt();

    if (payloadLength < 0) {
      throw new ProtocolDecodeException("Negative payload length: " + payloadLength);
    }
    if (payloadLength > maxPayloadBytes) {
      throw new ProtocolDecodeException(
          "Payload length %d exceeds maximum %d".formatted(payloadLength, maxPayloadBytes));
    }
    if (buffer.remaining() < payloadLength) {
      throw new ProtocolDecodeException("Response payload truncated");
    }

    StatusCode status = StatusCode.fromWire(statusWire);
    byte[] payload = new byte[payloadLength];
    buffer.get(payload);
    return new Response(version, correlationId, status, payload);
  }
}
