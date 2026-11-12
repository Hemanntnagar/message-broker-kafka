package com.javastream.security;

import com.javastream.exception.ConfigurationException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.security.KeyStore;

/** Creates TLS server sockets when {@link TlsSettings#enabled()} is true. */
public final class TlsServerSockets {

    private TlsServerSockets() {}

    public static SSLServerSocket openBound(TlsSettings tls, String listenHost, int listenPort, int backlog)
            throws IOException {
        if (!tls.enabled()) {
            throw new com.javastream.exception.ConfigurationException("TLS is not enabled");
        }
        if (tls.keystorePath().isEmpty()) {
            throw new ConfigurationException("network.tls.enabled=true requires network.tls.keystore.path");
        }
        SSLServerSocketFactory factory = sslContext(tls).getServerSocketFactory();
        SSLServerSocket socket = (SSLServerSocket) factory.createServerSocket();
        socket.setReuseAddress(true);
        socket.bind(new InetSocketAddress(listenHost, listenPort), backlog);
        socket.setNeedClientAuth(false);
        return socket;
    }

    private static SSLContext sslContext(TlsSettings tls) throws IOException {
        char[] password = tls.keystorePassword().toCharArray();
        try {
            KeyStore keyStore = KeyStore.getInstance(tls.keystoreType());
            try (InputStream in = Files.newInputStream(tls.keystorePath().get())) {
                keyStore.load(in, password);
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, password);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(kmf.getKeyManagers(), null, null);
            return context;
        } catch (Exception e) {
            throw new ConfigurationException("Failed to load TLS keystore: " + tls.keystorePath().get(), e);
        }
    }
}
