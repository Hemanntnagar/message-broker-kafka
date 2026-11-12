package com.javastream.security;

import java.nio.file.Path;
import java.util.Optional;

/**
 * TLS termination settings for the client listener.
 * <p>
 * JavaStream uses plain {@link java.net.ServerSocket} by default. When TLS is enabled, the broker wraps the
 * acceptor with {@link javax.net.ssl.SSLServerSocket} using a JDK keystore. Mutual TLS (client certificates) is
 * not supported in v1.
 */
public final class TlsSettings {

    private final boolean enabled;
    private final Path keystorePath;
    private final String keystorePassword;
    private final String keystoreType;

    public TlsSettings(boolean enabled, Path keystorePath, String keystorePassword, String keystoreType) {
        this.enabled = enabled;
        this.keystorePath = keystorePath;
        this.keystorePassword = keystorePassword == null ? "" : keystorePassword;
        this.keystoreType = keystoreType == null || keystoreType.isBlank() ? "PKCS12" : keystoreType;
    }

    public static TlsSettings disabled() {
        return new TlsSettings(false, null, "", "PKCS12");
    }

    public boolean enabled() {
        return enabled;
    }

    public Optional<Path> keystorePath() {
        return Optional.ofNullable(keystorePath);
    }

    public String keystorePassword() {
        return keystorePassword;
    }

    public String keystoreType() {
        return keystoreType;
    }
}
