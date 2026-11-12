package com.javastream.security;

import java.util.Optional;

/** Validates username and password against the configured credential store. */
public final class ClientAuthenticator {

    private final boolean required;
    private final UserCredentialStore store;

    public ClientAuthenticator(boolean required, UserCredentialStore store) {
        this.required = required;
        this.store = store == null ? UserCredentialStore.empty() : store;
    }

    public boolean authenticationRequired() {
        return required;
    }

    public Optional<String> authenticate(String username, char[] password) {
        if (username == null || username.isBlank()) {
            return Optional.empty();
        }
        if (password == null || password.length == 0) {
            return Optional.empty();
        }
        Optional<String> hash = store.lookupHash(username);
        if (hash.isEmpty()) {
            return Optional.empty();
        }
        if (!PasswordHasher.verifyPassword(password, hash.get())) {
            return Optional.empty();
        }
        return Optional.of(username);
    }
}
