package com.javastream.config;

import com.javastream.exception.ConfigurationException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

/**
 * Loads layered configuration: defaults, optional file, then environment overrides.
 */
public final class ConfigLoader {

    private static final String DEFAULT_RESOURCE = "javastream-default.properties";
    private static final String CONFIG_FILE_PROPERTY = "javastream.config.file";

    private ConfigLoader() {
    }

    public static JavaStreamConfig load() {
        return load(null);
    }

    /** Merged properties (defaults, optional file, environment) for storage tuning. */
    public static Properties loadPropertiesForStorage() {
        Properties props = new Properties();
        loadDefaults(props);
        Path file = resolveConfigFile(null);
        if (file != null) {
            loadFromFile(props, file);
        }
        EnvironmentConfigSource.apply(props);
        return props;
    }

    public static JavaStreamConfig load(Path optionalConfigFile) {
        Properties props = new Properties();
        loadDefaults(props);
        Path file = resolveConfigFile(optionalConfigFile);
        if (file != null) {
            loadFromFile(props, file);
        }
        EnvironmentConfigSource.apply(props);
        return buildConfig(props);
    }

    private static void loadDefaults(Properties props) {
        try (InputStream in = ConfigLoader.class.getClassLoader().getResourceAsStream(DEFAULT_RESOURCE)) {
            if (in == null) {
                throw new ConfigurationException("Missing classpath resource: " + DEFAULT_RESOURCE);
            }
            props.load(in);
        } catch (IOException e) {
            throw new ConfigurationException("Failed to load default configuration", e);
        }
    }

    private static Path resolveConfigFile(Path explicit) {
        if (explicit != null) {
            return explicit;
        }
        String fromSys = System.getProperty(CONFIG_FILE_PROPERTY);
        if (fromSys != null && !fromSys.isBlank()) {
            return Path.of(fromSys);
        }
        String fromEnv = System.getenv("JAVASTREAM_CONFIG_FILE");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return Path.of(fromEnv);
        }
        return null;
    }

    private static void loadFromFile(Properties props, Path file) {
        if (!Files.isRegularFile(file)) {
            throw new ConfigurationException("Configuration file does not exist: " + file);
        }
        try (InputStream in = Files.newInputStream(file)) {
            props.load(in);
        } catch (IOException e) {
            throw new ConfigurationException("Failed to read configuration file: " + file, e);
        }
    }

    private static JavaStreamConfig buildConfig(Properties props) {
        try {
            return new JavaStreamConfig(
                    props.getProperty("environment", "development"),
                    parseInt(props.getProperty("broker.id", "0"), "broker.id"),
                    props.getProperty("network.listen.host", "0.0.0.0"),
                    parseInt(props.getProperty("network.listen.port", "9092"), "network.listen.port"),
                    props.getProperty("storage.data.dir", "./data"),
                    Boolean.parseBoolean(props.getProperty("cluster.enabled", "false")),
                    parseInt(props.getProperty("topic.default.partition.count", "1"), "topic.default.partition.count"));
        } catch (NumberFormatException e) {
            throw new ConfigurationException("Invalid numeric configuration value", e);
        }
    }

    private static int parseInt(String value, String key) {
        Objects.requireNonNull(value, key);
        return Integer.parseInt(value.trim());
    }
}
