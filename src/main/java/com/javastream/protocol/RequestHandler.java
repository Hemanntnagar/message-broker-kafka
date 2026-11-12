package com.javastream.protocol;

import java.nio.ByteBuffer;

/**
 * Decodes wire requests and delegates to broker services (implementation in later phases).
 */
public interface RequestHandler {

    ByteBuffer handle(ByteBuffer request);
}
