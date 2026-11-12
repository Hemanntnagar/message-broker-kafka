package com.javastream.admin;

import com.javastream.config.ConfigLoader;
import com.javastream.config.JavaStreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Secondary entry point for operator tooling (stub in Phase 0).
 */
public final class AdminCliApplication {

    private static final Logger log = LoggerFactory.getLogger(AdminCliApplication.class);

    private AdminCliApplication() {
    }

    public static void main(String[] args) {
        JavaStreamConfig config = ConfigLoader.load();
        log.info("JavaStream admin CLI (Phase 0) — target brokerId={}, env={}",
                config.brokerId(), config.environment());
        if (args.length == 0) {
            log.info("Usage: admin <command> (commands not implemented yet)");
            return;
        }
        log.info("Command received: {} (not implemented in Phase 0)", args[0]);
    }
}
