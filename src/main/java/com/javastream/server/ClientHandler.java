package com.javastream.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.function.Consumer;

/**
 * Handles a single TCP client connection using length-prefixed frames (4-byte big-endian size).
 * Phase 1 responds with an empty payload acknowledgment; broker logic arrives in later phases.
 */
public final class ClientHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

    private final Socket socket;
    private final int socketTimeoutMs;
    private final int maxRequestBytes;
    private final Consumer<ClientHandler> onClose;

    ClientHandler(Socket socket, int socketTimeoutMs, int maxRequestBytes, Consumer<ClientHandler> onClose) {
        this.socket = socket;
        this.socketTimeoutMs = socketTimeoutMs;
        this.maxRequestBytes = maxRequestBytes;
        this.onClose = onClose;
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
        DataInputStream in = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        while (!socket.isClosed()) {
            int length;
            try {
                length = in.readInt();
            } catch (EOFException e) {
                return;
            }

            if (length < 0 || length > maxRequestBytes) {
                log.warn("Malformed request length {} from {}; closing connection", length, remoteAddress());
                return;
            }

            byte[] payload = in.readNBytes(length);
            if (payload.length != length) {
                log.warn("Incomplete request from {} (expected {} bytes, got {})",
                        remoteAddress(), length, payload.length);
                return;
            }

            // Phase 1: acknowledge without interpreting payload (no storage / produce-consume yet).
            byte[] responsePayload = new byte[0];
            out.writeInt(responsePayload.length);
            out.write(responsePayload);
            out.flush();
        }
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
