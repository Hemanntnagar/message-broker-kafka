package com.javastream.security;

import com.javastream.protocol.OperationCode;

/** Centralized authentication and authorization checks for client requests. */
public final class SecurityPolicy {

    private final SecuritySettings settings;

    public SecurityPolicy(SecuritySettings settings) {
        this.settings = settings == null ? SecuritySettings.disabled() : settings;
    }

    public SecuritySettings settings() {
        return settings;
    }

    public boolean isAuthenticationRequired(OperationCode operation) {
        return settings.clientAuthEnabled() && operation != OperationCode.PING && operation != OperationCode.CLIENT_AUTH;
    }

    public boolean isAuthenticated(ConnectionSecurityContext ctx) {
        if (!settings.clientAuthEnabled()) {
            return true;
        }
        return ctx != null && ctx.isAuthenticated();
    }

    public boolean authorizeProduce(ConnectionSecurityContext ctx, String topic) {
        return settings.accessControl().isAllowed(principal(ctx), topic, TopicPermission.PRODUCE);
    }

    public boolean authorizeConsume(ConnectionSecurityContext ctx, String topic) {
        return settings.accessControl().isAllowed(principal(ctx), topic, TopicPermission.CONSUME);
    }

    public boolean authorizeAdmin(ConnectionSecurityContext ctx, String topic) {
        return settings.accessControl().isAllowed(principal(ctx), topic, TopicPermission.ADMIN);
    }

    private static String principal(ConnectionSecurityContext ctx) {
        if (ctx == null) {
            return ConnectionSecurityContext.ANONYMOUS;
        }
        return ctx.principalOrAnonymous();
    }
}
