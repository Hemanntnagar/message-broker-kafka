package com.javastream.protocol;

import com.javastream.exception.JavaStreamException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/** Serializes {@link Request} and {@link Response} into versioned binary frames. */
public final class ProtocolEncoder {

  private final int maxPayloadBytes;

  public ProtocolEncoder() {
    this(Integer.MAX_VALUE);
  }

  public ProtocolEncoder(int maxPayloadBytes) {
    if (maxPayloadBytes < 0) {
      throw new JavaStreamException("maxPayloadBytes must be non-negative");
    }
    this.maxPayloadBytes = maxPayloadBytes;
  }

  public byte[] encodeRequest(Request request) {
    Objects.requireNonNull(request, "request");
    validatePayload(request.payloadLength());
    return encodeFrame(
        request.version(),
        request.correlationId(),
        request.operation().wireValue(),
        request.payload());
  }

  public byte[] encodeResponse(Response response) {
    Objects.requireNonNull(response, "response");
    validatePayload(response.payloadLength());
    return encodeFrame(
        response.version(),
        response.correlationId(),
        response.status().wireValue(),
        response.payload());
  }

  private byte[] encodeFrame(short version, int correlationId, short codeField, byte[] payload) {
    int frameSize = ProtocolConstants.REQUEST_HEADER_BYTES + payload.length;
    ByteBuffer buffer = ByteBuffer.allocate(frameSize).order(ByteOrder.BIG_ENDIAN);
    buffer.putInt(ProtocolConstants.MAGIC);
    buffer.putShort(version);
    buffer.putInt(correlationId);
    buffer.putShort(codeField);
    buffer.putInt(payload.length);
    buffer.put(payload);
    return buffer.array();
  }

  private void validatePayload(int length) {
    if (length < 0 || length > maxPayloadBytes) {
      throw new JavaStreamException(
          "Payload length %d exceeds allowed maximum %d".formatted(length, maxPayloadBytes));
    }
  }
}
