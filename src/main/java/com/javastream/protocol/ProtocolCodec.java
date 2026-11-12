package com.javastream.protocol;

import java.nio.ByteBuffer;

/**
 * Framing and serialization boundary between the network layer and business logic.
 */
public interface ProtocolCodec {

    ByteBuffer encode(Object message);

    Object decode(ByteBuffer buffer);
}
