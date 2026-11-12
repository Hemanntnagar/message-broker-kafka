package com.javastream.security;

import com.javastream.exception.ConfigurationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecurityConfigLoaderTest {

  @Test
  void loadsHashedUsersAndAclRules() {
    String hash = PasswordHasher.hashPassword("secret".toCharArray());
    Properties props = new Properties();
    props.setProperty("security.client.auth.enabled", "true");
    props.setProperty("security.user.alice.password.hash", hash);
    props.setProperty("security.acl.enabled", "true");
    props.setProperty("security.acl.rules", "alice:orders:produce,consume");
    SecuritySettings settings = SecurityConfigLoader.fromProperties(props);
    assertTrue(settings.clientAuthEnabled());
    assertTrue(settings.accessControl().enabled());
    assertTrue(settings.authenticator().authenticate("alice", "secret".toCharArray()).isPresent());
  }

  @Test
  void rejectsAuthWithoutUsers() {
    Properties props = new Properties();
    props.setProperty("security.client.auth.enabled", "true");
    assertThrows(ConfigurationException.class, () -> SecurityConfigLoader.fromProperties(props));
  }

  @Test
  void loadsUsersFile(@TempDir Path temp) throws Exception {
    String hash = PasswordHasher.hashPassword("pw".toCharArray());
    Path users = temp.resolve("users.cnf");
    Files.writeString(users, "bob=" + hash + "\n", StandardCharsets.UTF_8);
    Properties props = new Properties();
    props.setProperty("security.client.auth.enabled", "true");
    props.setProperty("security.users.file", users.toString());
    SecuritySettings settings = SecurityConfigLoader.fromProperties(props);
    assertTrue(settings.authenticator().authenticate("bob", "pw".toCharArray()).isPresent());
  }
}
