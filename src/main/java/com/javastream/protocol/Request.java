package com.javastream.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Decoded client request (header fields + opaque payload). */
public final class Request {

  private final short version;
  private final int correlationId;
  private final OperationCode operation;
  private final byte[] payload;

  public Request(short version, int correlationId, OperationCode operation, byte[] payload) {
    this.version = version;
    this.correlationId = correlationId;
    this.operation = Objects.requireNonNull(operation, "operation");
    this.payload = payload == null ? new byte[0] : payload.clone();
  }

  public short version() {
    return version;
  }

  public int correlationId() {
    return correlationId;
  }

  public OperationCode operation() {
    return operation;
  }

  public byte[] payload() {
    return payload.clone();
  }

  public int payloadLength() {
    return payload.length;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Request other)) {
      return false;
    }
    return version == other.version
        && correlationId == other.correlationId
        && operation == other.operation
        && Arrays.equals(payload, other.payload);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(version, correlationId, operation);
    result = 31 * result + Arrays.hashCode(payload);
    return result;
  }

  @Override
  public String toString() {
    return "Request{version="
        + version
        + ", correlationId="
        + correlationId
        + ", operation="
        + operation
        + ", payloadLength="
        + payload.length
        + '}';
  }
}
