package com.javastream.security;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PasswordHasherTest {

    @Test
    void hashesAndVerifiesPassword() {
        char[] password = "s3cret".toCharArray();
        String hash = PasswordHasher.hashPassword(password);
        assertTrue(PasswordHasher.verifyPassword(password, hash));
        assertFalse(PasswordHasher.verifyPassword("wrong".toCharArray(), hash));
    }
}
