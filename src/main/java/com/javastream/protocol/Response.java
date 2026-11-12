package com.javastream.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Encoded server response (header fields + opaque payload). */
public final class Response {

  private final short version;
  private final int correlationId;
  private final StatusCode status;
  private final byte[] payload;

  public Response(short version, int correlationId, StatusCode status, byte[] payload) {
    this.version = version;
    this.correlationId = correlationId;
    this.status = Objects.requireNonNull(status, "status");
    this.payload = payload == null ? new byte[0] : payload.clone();
  }

  public static Response ok(int correlationId, byte[] payload) {
    return new Response(ProtocolConstants.VERSION_1, correlationId, StatusCode.OK, payload);
  }

  public short version() {
    return version;
  }

  public int correlationId() {
    return correlationId;
  }

  public StatusCode status() {
    return status;
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
    if (!(o instanceof Response other)) {
      return false;
    }
    return version == other.version
        && correlationId == other.correlationId
        && status == other.status
        && Arrays.equals(payload, other.payload);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(version, correlationId, status);
    result = 31 * result + Arrays.hashCode(payload);
    return result;
  }

  @Override
  public String toString() {
    return "Response{version="
        + version
        + ", correlationId="
        + correlationId
        + ", status="
        + status
        + ", payloadLength="
        + payload.length
        + '}';
  }
}
