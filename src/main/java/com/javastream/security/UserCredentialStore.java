package com.javastream.security;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** In-memory username → password-hash store (loaded from configuration). */
public final class UserCredentialStore {

    private final Map<String, String> usernameToHash;

    private UserCredentialStore(Map<String, String> usernameToHash) {
        this.usernameToHash = Map.copyOf(usernameToHash);
    }

    public static UserCredentialStore empty() {
        return new UserCredentialStore(Map.of());
    }

    public static UserCredentialStore ofHashed(Map<String, String> usernameToHash) {
        return new UserCredentialStore(new HashMap<>(usernameToHash));
    }

    public static UserCredentialStore bootstrapFromPlain(Map<String, char[]> plainPasswords) {
        Map<String, String> hashed = new HashMap<>();
        for (var entry : plainPasswords.entrySet()) {
            hashed.put(entry.getKey(), PasswordHasher.hashPassword(entry.getValue()));
        }
        return new UserCredentialStore(hashed);
    }

    public boolean isEmpty() {
        return usernameToHash.isEmpty();
    }

    public Optional<String> lookupHash(String username) {
        if (username == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(usernameToHash.get(username));
    }

    public java.util.Set<String> usernames() {
        return Collections.unmodifiableSet(usernameToHash.keySet());
    }
}
