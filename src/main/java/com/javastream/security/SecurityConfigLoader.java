package com.javastream.security;

import com.javastream.exception.ConfigurationException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Builds {@link SecuritySettings} from merged configuration properties. */
public final class SecurityConfigLoader {

    private SecurityConfigLoader() {}

    public static SecuritySettings fromProperties(Properties props) {
        boolean authEnabled = Boolean.parseBoolean(props.getProperty("security.client.auth.enabled", "false"));
        boolean aclEnabled = Boolean.parseBoolean(props.getProperty("security.acl.enabled", "false"));
        int maxMessageBytes = parsePositiveInt(props.getProperty("security.max.message.bytes"), 1_048_576);
        int maxRequestBytes = parsePositiveInt(props.getProperty("network.max.request.bytes"), 1_048_576);
        int maxConnections = parsePositiveInt(props.getProperty("security.max.connections"), 2_048);

        UserCredentialStore store = loadCredentialStore(props, authEnabled);
        ClientAuthenticator authenticator = new ClientAuthenticator(authEnabled, store);
        TopicAccessControl acl = aclEnabled ? loadAcl(props) : TopicAccessControl.disabled();

        boolean tlsEnabled = Boolean.parseBoolean(props.getProperty("network.tls.enabled", "false"));
        String keystorePath = props.getProperty("network.tls.keystore.path", "").trim();
        String keystorePassword = props.getProperty("network.tls.keystore.password", "");
        String keystoreType = props.getProperty("network.tls.keystore.type", "PKCS12");
        Path path = keystorePath.isBlank() ? null : Path.of(keystorePath);
        if (tlsEnabled && path == null) {
            throw new ConfigurationException("network.tls.enabled=true requires network.tls.keystore.path");
        }
        TlsSettings tls = new TlsSettings(tlsEnabled, path, keystorePassword, keystoreType);

        if (authEnabled && store.isEmpty()) {
            throw new ConfigurationException(
                    "security.client.auth.enabled=true requires security.users.file or security.user.*.password.hash entries");
        }

        return new SecuritySettings(
                authEnabled,
                authenticator,
                acl,
                maxMessageBytes,
                maxRequestBytes,
                maxConnections,
                tls);
    }

    private static UserCredentialStore loadCredentialStore(Properties props, boolean authEnabled) {
        if (!authEnabled) {
            return UserCredentialStore.empty();
        }
        Map<String, String> hashed = new HashMap<>();
        String usersFile = props.getProperty("security.users.file", "").trim();
        if (!usersFile.isBlank()) {
            hashed.putAll(loadUsersFile(Path.of(usersFile)));
        }
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("security.user.") && key.endsWith(".password.hash")) {
                String username = key.substring("security.user.".length(), key.length() - ".password.hash".length());
                String hash = props.getProperty(key, "").trim();
                if (!username.isBlank() && !hash.isBlank()) {
                    hashed.put(username, hash);
                }
            }
        }
        return UserCredentialStore.ofHashed(hashed);
    }

    private static Map<String, String> loadUsersFile(Path file) {
        if (!Files.isRegularFile(file)) {
            throw new ConfigurationException("security.users.file does not exist: " + file);
        }
        Map<String, String> users = new HashMap<>();
        try {
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                int eq = trimmed.indexOf('=');
                if (eq <= 0) {
                    throw new ConfigurationException("Invalid users file line: " + line);
                }
                String user = trimmed.substring(0, eq).trim();
                String hash = trimmed.substring(eq + 1).trim();
                if (user.isBlank() || hash.isBlank()) {
                    throw new ConfigurationException("Invalid users file line: " + line);
                }
                users.put(user, hash);
            }
        } catch (IOException e) {
            throw new ConfigurationException("Failed to read security.users.file: " + file, e);
        }
        return users;
    }

    private static TopicAccessControl loadAcl(Properties props) {
        List<AclRule> rules = new ArrayList<>();
        String rulesProperty = props.getProperty("security.acl.rules", "").trim();
        if (!rulesProperty.isBlank()) {
            for (String entry : rulesProperty.split(";")) {
                String trimmed = entry.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                rules.add(parseAclRule(trimmed));
            }
        }
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("security.acl.rule.")) {
                rules.add(parseAclRule(props.getProperty(key, "").trim()));
            }
        }
        if (rules.isEmpty()) {
            throw new ConfigurationException("security.acl.enabled=true requires security.acl.rules");
        }
        return TopicAccessControl.of(true, rules);
    }

    private static AclRule parseAclRule(String spec) {
        String[] parts = spec.split(":");
        if (parts.length != 3) {
            throw new ConfigurationException("ACL rule must be principal:topicPattern:perm1,perm2 — got: " + spec);
        }
        String principal = parts[0].trim();
        String topicPattern = parts[1].trim();
        String[] permTokens = parts[2].split(",");
        List<TopicPermission> permissions = new ArrayList<>();
        for (String token : permTokens) {
            if (!token.isBlank()) {
                permissions.add(TopicPermission.parse(token));
            }
        }
        if (permissions.isEmpty()) {
            throw new ConfigurationException("ACL rule missing permissions: " + spec);
        }
        Set<TopicPermission> permissionSet = EnumSet.copyOf(permissions);
        return new AclRule(principal, topicPattern, permissionSet);
    }

    private static int parsePositiveInt(String raw, int defaultValue) {
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        int value = Integer.parseInt(raw.trim());
        if (value <= 0) {
            throw new ConfigurationException("Expected positive int, got " + raw);
        }
        return value;
    }
}
