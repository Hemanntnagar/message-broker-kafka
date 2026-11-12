package com.javastream.storage.log;

import java.util.List;

/**
 * Append-only partition log: sequential {@link #append} and offset-based {@link #read}.
 */
public interface MessageLog {

  /**
   * Assigns the next partition offset, appends the message, and returns that offset.
   */
  long append(LogMessage message);

  List<LogMessage> read(long offset, int maxMessages);

  long logEndOffset();

  long recordCount();

  void flush();

  void close();
}
