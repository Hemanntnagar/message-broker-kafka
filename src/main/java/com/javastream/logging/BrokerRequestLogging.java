package com.javastream.logging;

import com.javastream.protocol.OperationCode;
import com.javastream.protocol.Response;
import com.javastream.protocol.StatusCode;
import org.slf4j.Logger;
import org.slf4j.MDC;

import java.util.Map;

/** Structured request logging via MDC (consumed by Logback JSON encoder). */
public final class BrokerRequestLogging {

    public static final String MDC_BROKER_ID = "brokerId";
    public static final String MDC_CLIENT_ID = "clientId";
    public static final String MDC_CORRELATION_ID = "correlationId";
    public static final String MDC_OPERATION = "operation";
    public static final String MDC_STATUS = "status";
    public static final String MDC_ERROR = "error";
    public static final String MDC_PRINCIPAL = "principal";

    private BrokerRequestLogging() {}

    public static void logRequest(
            Logger log,
            int brokerId,
            String clientId,
            int correlationId,
            OperationCode operation,
            Response response,
            String errorMessage) {
        logRequest(log, brokerId, clientId, correlationId, operation, response, errorMessage, null);
    }

    public static void logRequest(
            Logger log,
            int brokerId,
            String clientId,
            int correlationId,
            OperationCode operation,
            Response response,
            String errorMessage,
            String principal) {
        Map<String, String> previous = MDC.getCopyOfContextMap();
        try {
            MDC.put(MDC_BROKER_ID, String.valueOf(brokerId));
            MDC.put(MDC_CLIENT_ID, clientId == null ? "unknown" : clientId);
            MDC.put(MDC_CORRELATION_ID, String.valueOf(correlationId));
            MDC.put(MDC_OPERATION, operation.name());
            MDC.put(MDC_STATUS, response.status().name());
            if (principal != null && !principal.isBlank()) {
                MDC.put(MDC_PRINCIPAL, principal);
            }
            if (errorMessage != null && !errorMessage.isBlank()) {
                MDC.put(MDC_ERROR, errorMessage);
            }
            if (response.status() == StatusCode.OK) {
                log.info("broker request completed");
            } else {
                log.warn("broker request failed");
            }
        } finally {
            MDC.clear();
            if (previous != null) {
                MDC.setContextMap(previous);
            }
        }
    }
}
