package com.javastream.protocol;

/**
 * JavaStream binary wire protocol layout (version 1).
 * <p>
 * All multi-byte integers are big-endian. TCP carries a byte stream; frames are not delimited by the
 * transport—use {@link ProtocolDecoder} to reassemble partial reads and coalesced reads.
 *
 * See {@code PROTOCOL.md} in the repository root.
 */
public final class ProtocolConstants {

  /** Magic {@code 0x4A535452} ({@code "JSTR"}). */
  public static final int MAGIC = 0x4A535452;

  /** Supported wire protocol version. */
  public static final short VERSION_1 = 1;

  public static final int REQUEST_HEADER_BYTES = 16;
  public static final int RESPONSE_HEADER_BYTES = 16;

  private ProtocolConstants() {}
}
