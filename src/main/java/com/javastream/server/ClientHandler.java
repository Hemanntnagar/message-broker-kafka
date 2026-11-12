package com.javastream.server;

import com.javastream.exception.ProtocolDecodeException;
import com.javastream.protocol.ProtocolConstants;
import com.javastream.protocol.ProtocolDecoder;
import com.javastream.protocol.ProtocolEncoder;
import com.javastream.protocol.Request;
import com.javastream.protocol.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.function.Consumer;

/**
 * Handles a single TCP client connection using the JavaStream v1 binary protocol.
 * Broker semantics for most operations are stubbed until later phases; framing is fully enforced.
 */
public final class ClientHandler implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);
  private static final int READ_CHUNK_BYTES = 8_192;

  private final Socket socket;
  private final int socketTimeoutMs;
  private final int maxRequestBytes;
  private final Consumer<ClientHandler> onClose;
  private final com.javastream.broker.BrokerRequestDispatcher dispatcher;

  ClientHandler(
      Socket socket,
      int socketTimeoutMs,
      int maxRequestBytes,
      Consumer<ClientHandler> onClose,
      com.javastream.broker.BrokerRequestDispatcher dispatcher) {
    this.socket = socket;
    this.socketTimeoutMs = socketTimeoutMs;
    this.maxRequestBytes = maxRequestBytes;
    this.onClose = onClose;
    this.dispatcher = dispatcher;
  }

  @Override
  public void run() {
    String remote = remoteAddress();
    log.info("Client connected: {}", remote);
    try {
      socket.setSoTimeout(socketTimeoutMs);
      processRequests();
    } catch (SocketTimeoutException e) {
      log.debug("Client idle timeout: {}", remote);
    } catch (SocketException e) {
      if (socket.isClosed()) {
        log.debug("Client socket closed: {}", remote);
      } else {
        log.warn("Client socket error from {}: {}", remote, e.toString());
      }
    } catch (IOException e) {
      log.warn("I/O error handling client {}: {}", remote, e.toString());
    } finally {
      closeQuietly();
      onClose.accept(this);
      log.info("Client disconnected: {}", remote);
    }
  }

  private void processRequests() throws IOException {
    InputStream in = socket.getInputStream();
    OutputStream out = socket.getOutputStream();
    ProtocolDecoder decoder = new ProtocolDecoder(maxRequestBytes, ProtocolConstants.VERSION_1);
    ProtocolEncoder encoder = new ProtocolEncoder(maxRequestBytes);
    byte[] chunk = new byte[READ_CHUNK_BYTES];

    while (!socket.isClosed()) {
      int read;
      try {
        read = in.read(chunk);
      } catch (EOFException e) {
        return;
      }
      if (read < 0) {
        return;
      }
      if (read == 0) {
        continue;
      }

      try {
        decoder.feed(chunk, 0, read);
      } catch (ProtocolDecodeException e) {
        log.warn("Protocol error from {}: {}", remoteAddress(), e.getMessage());
        return;
      }

      List<Request> requests;
      try {
        requests = decoder.decodeAvailable();
      } catch (ProtocolDecodeException e) {
        log.warn("Protocol error from {}: {}", remoteAddress(), e.getMessage());
        return;
      }

      for (Request request : requests) {
        Response response = dispatch(request);
        out.write(encoder.encodeResponse(response));
        out.flush();
      }
    }
  }

  private Response dispatch(Request request) {
    return dispatcher.dispatch(request);
  }

  void closeQuietly() {
    try {
      if (!socket.isClosed()) {
        socket.close();
      }
    } catch (IOException e) {
      log.debug("Error closing client socket {}: {}", remoteAddress(), e.toString());
    }
  }

  private String remoteAddress() {
    return socket.getRemoteSocketAddress().toString();
  }
}
