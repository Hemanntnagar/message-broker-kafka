package com.javastream.security;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/** Per-TCP-connection authentication state. */
public final class ConnectionSecurityContext {

    public static final String ANONYMOUS = "anonymous";

    private final AtomicReference<String> principal = new AtomicReference<>();

    public Optional<String> principal() {
        return Optional.ofNullable(principal.get());
    }

    public String principalOrAnonymous() {
        return principal.get() == null ? ANONYMOUS : principal.get();
    }

    public void setPrincipal(String username) {
        principal.set(username);
    }

    public void clearPrincipal() {
        principal.set(null);
    }

    public boolean isAuthenticated() {
        return principal.get() != null;
    }
}
