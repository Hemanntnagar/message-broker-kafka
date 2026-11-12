package com.javastream.protocol;

import com.javastream.exception.ProtocolDecodeException;

/** Response status discriminator on the wire (uint16). */
public enum StatusCode {
  OK(0),
  BAD_REQUEST(1),
  NOT_FOUND(2),
  UNSUPPORTED_VERSION(3),
  PAYLOAD_TOO_LARGE(4),
  INTERNAL_ERROR(5),
  UNKNOWN_OPERATION(6),
  MALFORMED(7),
  FORBIDDEN(8);

  private final short wireValue;

  StatusCode(int wireValue) {
    this.wireValue = (short) wireValue;
  }

  public short wireValue() {
    return wireValue;
  }

  public static StatusCode fromWire(short wireValue) {
    for (StatusCode code : values()) {
      if (code.wireValue == wireValue) {
        return code;
      }
    }
    throw new ProtocolDecodeException("Unknown status code: " + (wireValue & 0xFFFF));
  }
}
