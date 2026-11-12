package com.javastream.admin;

import java.util.Objects;
import java.util.Optional;

/** Validates operator credentials for administrative mutations. */
public final class AdminAuthority {

    private final boolean required;
    private final String expectedToken;

    public AdminAuthority(boolean required, String expectedToken) {
        this.required = required;
        this.expectedToken = expectedToken == null ? "" : expectedToken;
    }

    public static AdminAuthority disabled() {
        return new AdminAuthority(false, "");
    }

    public static AdminAuthority fromConfig(String token) {
        if (token == null || token.isBlank()) {
            return disabled();
        }
        return new AdminAuthority(true, token.trim());
    }

    public boolean isRequired() {
        return required;
    }

    public boolean authorize(Optional<String> presentedToken) {
        if (!required) {
            return true;
        }
        String token = presentedToken.filter(t -> !t.isBlank()).orElse("");
        return Objects.equals(expectedToken, token);
    }

    public boolean authorize(String presentedToken) {
        return authorize(Optional.ofNullable(presentedToken));
    }
}
