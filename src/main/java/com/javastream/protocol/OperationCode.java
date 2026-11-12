package com.javastream.protocol;

import com.javastream.exception.ProtocolDecodeException;

/** Request operation discriminator on the wire (uint16). */
public enum OperationCode {
  PING(1),
  CREATE_TOPIC(2),
  LIST_TOPICS(3),
  PRODUCE(4),
  FETCH(5),
  DELETE_TOPIC(6),
  GET_METADATA(7),
  CONSUMER_ACK(8);

  private final short wireValue;

  OperationCode(int wireValue) {
    this.wireValue = (short) wireValue;
  }

  public short wireValue() {
    return wireValue;
  }

  public static OperationCode fromWire(short wireValue) {
    for (OperationCode code : values()) {
      if (code.wireValue == wireValue) {
        return code;
      }
    }
    throw new ProtocolDecodeException("Unknown operation code: " + (wireValue & 0xFFFF));
  }
}
