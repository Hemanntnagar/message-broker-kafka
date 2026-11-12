package com.javastream.security;

import com.javastream.common.Preconditions;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Objects;

/** PBKDF2-HMAC-SHA256 password hashing (no external dependencies). */
public final class PasswordHasher {

    public static final String ALGORITHM = "PBKDF2WithHmacSHA256";
    public static final int DEFAULT_ITERATIONS = 210_000;
    public static final int KEY_LENGTH_BITS = 256;
    public static final int SALT_BYTES = 16;

    private static final String PREFIX = "pbkdf2_sha256$";

    private PasswordHasher() {}

    public static String hashPassword(char[] plainPassword) {
        Objects.requireNonNull(plainPassword, "plainPassword");
        byte[] salt = new byte[SALT_BYTES];
        new SecureRandom().nextBytes(salt);
        byte[] derived = pbkdf2(plainPassword, salt, DEFAULT_ITERATIONS);
        return PREFIX
                + DEFAULT_ITERATIONS
                + "$"
                + Base64.getEncoder().encodeToString(salt)
                + "$"
                + Base64.getEncoder().encodeToString(derived);
    }

    public static boolean verifyPassword(char[] plainPassword, String encodedHash) {
        Preconditions.requireNonBlank(encodedHash, "encodedHash");
        if (!encodedHash.startsWith(PREFIX)) {
            return false;
        }
        String[] parts = encodedHash.substring(PREFIX.length()).split("\\$");
        if (parts.length != 3) {
            return false;
        }
        int iterations;
        try {
            iterations = Integer.parseInt(parts[0]);
        } catch (NumberFormatException e) {
            return false;
        }
        byte[] salt;
        byte[] expected;
        try {
            salt = Base64.getDecoder().decode(parts[1]);
            expected = Base64.getDecoder().decode(parts[2]);
        } catch (IllegalArgumentException e) {
            return false;
        }
        byte[] actual = pbkdf2(plainPassword, salt, iterations);
        return constantTimeEquals(expected, actual);
    }

    private static byte[] pbkdf2(char[] password, byte[] salt, int iterations) {
        try {
            PBEKeySpec spec = new PBEKeySpec(password, salt, iterations, KEY_LENGTH_BITS);
            SecretKeyFactory factory = SecretKeyFactory.getInstance(ALGORITHM);
            return factory.generateSecret(spec).getEncoded();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("PBKDF2 unavailable", e);
        }
    }

    private static boolean constantTimeEquals(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return false;
        }
        int diff = 0;
        for (int i = 0; i < a.length; i++) {
            diff |= a[i] ^ b[i];
        }
        return diff == 0;
    }

    /** Redacts a password for logs (never logs raw credentials). */
    public static String redact(String value) {
        if (value == null || value.isEmpty()) {
            return "<empty>";
        }
        return "<redacted len=" + value.getBytes(StandardCharsets.UTF_8).length + ">";
    }
}
