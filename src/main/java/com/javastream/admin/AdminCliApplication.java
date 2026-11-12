package com.javastream.admin;

import com.javastream.broker.BrokerServices;
import com.javastream.config.ConfigLoader;
import com.javastream.config.JavaStreamConfig;
import com.javastream.monitoring.MonitoringBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Secondary entry point for operator tooling.
 */
public final class AdminCliApplication {

    private static final Logger log = LoggerFactory.getLogger(AdminCliApplication.class);

    private AdminCliApplication() {
    }

    public static void main(String[] args) {
        JavaStreamConfig config = ConfigLoader.load();
        if (args.length == 0) {
            printUsage();
            return;
        }
        AdminCommand command;
        try {
            command = AdminCommand.valueOf(args[0].toUpperCase());
        } catch (IllegalArgumentException e) {
            log.error("Unknown command: {}", args[0]);
            printUsage();
            return;
        }
        Map<String, String> params = parseArgs(args, 1);
        BrokerServices services = new BrokerServices(config);
        MonitoringBundle monitoring =
                MonitoringBundle.create(config, services, Optional.empty(), config.listenPort());
        BrokerAdminService admin = monitoring.adminService();
        try {
            Object result = admin.execute(command, params);
            log.info("{}", result);
        } catch (RuntimeException e) {
            log.error("Command failed: {}", e.getMessage());
        } finally {
            monitoring.close();
            services.close();
        }
    }

    private static Map<String, String> parseArgs(String[] args, int fromIndex) {
        Map<String, String> out = new HashMap<>();
        for (int i = fromIndex; i < args.length; i++) {
            String arg = args[i];
            int eq = arg.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException("Expected key=value argument, got: " + arg);
            }
            out.put(arg.substring(0, eq), arg.substring(eq + 1));
        }
        return out;
    }

    private static void printUsage() {
        log.info(
                "Usage: admin <COMMAND> [key=value ...] — commands: {}",
                String.join(", ", java.util.Arrays.stream(AdminCommand.values()).map(Enum::name).toList()));
    }
}
