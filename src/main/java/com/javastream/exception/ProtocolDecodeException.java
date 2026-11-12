package com.javastream.exception;

/** Framing or header validation failure while decoding the wire protocol. */
public class ProtocolDecodeException extends JavaStreamException {

  public ProtocolDecodeException(String message) {
    super(message);
  }
}
