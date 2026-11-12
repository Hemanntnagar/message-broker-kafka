package com.javastream.config;

import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Applies {@code JAVASTREAM_*} environment variables onto configuration properties.
 * <p>
 * Example: {@code JAVASTREAM_NETWORK_LISTEN_PORT=9093} maps to {@code network.listen.port}.
 */
public final class EnvironmentConfigSource {

    public static final String ENV_PREFIX = "JAVASTREAM_";

    private EnvironmentConfigSource() {
    }

    public static void apply(Properties target) {
        Map<String, String> env = System.getenv();
        env.entrySet().stream()
                .filter(e -> e.getKey().startsWith(ENV_PREFIX))
                .forEach(e -> target.setProperty(envKeyToPropertyKey(e.getKey()), e.getValue()));
    }

    static String envKeyToPropertyKey(String envKey) {
        String suffix = envKey.substring(ENV_PREFIX.length());
        return suffix.toLowerCase(Locale.ROOT).replace('_', '.');
    }
}
