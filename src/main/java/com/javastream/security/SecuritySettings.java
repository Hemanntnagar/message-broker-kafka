package com.javastream.security;

/** Immutable broker security configuration. */
public final class SecuritySettings {

    public static final int DEFAULT_MAX_MESSAGE_BYTES = 1_048_576;
    public static final int DEFAULT_MAX_CONNECTIONS = 2_048;
    public static final int DEFAULT_MAX_REQUEST_BYTES = 1_048_576;

    private final boolean clientAuthEnabled;
    private final ClientAuthenticator authenticator;
    private final TopicAccessControl accessControl;
    private final int maxMessageBytes;
    private final int maxRequestBytes;
    private final int maxConcurrentConnections;
    private final TlsSettings tls;

    public SecuritySettings(
            boolean clientAuthEnabled,
            ClientAuthenticator authenticator,
            TopicAccessControl accessControl,
            int maxMessageBytes,
            int maxRequestBytes,
            int maxConcurrentConnections,
            TlsSettings tls) {
        this.clientAuthEnabled = clientAuthEnabled;
        this.authenticator = authenticator;
        this.accessControl = accessControl;
        this.maxMessageBytes = Math.max(1, maxMessageBytes);
        this.maxRequestBytes = Math.max(ProtocolMaxFrame.MIN_REQUEST_BYTES, maxRequestBytes);
        this.maxConcurrentConnections = Math.max(1, maxConcurrentConnections);
        this.tls = tls == null ? TlsSettings.disabled() : tls;
    }

    public static SecuritySettings disabled() {
        return new SecuritySettings(
                false,
                new ClientAuthenticator(false, UserCredentialStore.empty()),
                TopicAccessControl.disabled(),
                DEFAULT_MAX_MESSAGE_BYTES,
                DEFAULT_MAX_REQUEST_BYTES,
                DEFAULT_MAX_CONNECTIONS,
                TlsSettings.disabled());
    }

    public boolean clientAuthEnabled() {
        return clientAuthEnabled;
    }

    public ClientAuthenticator authenticator() {
        return authenticator;
    }

    public TopicAccessControl accessControl() {
        return accessControl;
    }

    public int maxMessageBytes() {
        return maxMessageBytes;
    }

    public int maxRequestBytes() {
        return maxRequestBytes;
    }

    public int maxConcurrentConnections() {
        return maxConcurrentConnections;
    }

    public TlsSettings tls() {
        return tls;
    }
}
