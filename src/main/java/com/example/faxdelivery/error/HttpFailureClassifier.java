package com.example.faxdelivery.error;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLHandshakeException;

import org.springframework.web.reactive.function.client.WebClientResponseException;

/**
 * Turns anything thrown by a WebClient call into the taxonomy.
 *
 * <pre>
 * Response received
 *   401, 403                   Systemic   (auth/config broken)
 *   408, 429, 503              Transient, NOT processed
 *   permanent-statuses, 4xx    Permanent
 *   500                        ServerError (ambiguous, cap configurable)
 *   502, 504, other 5xx        Transient, AMBIGUOUS
 * No response
 *   connect refused/timeout, DNS, no route, TLS handshake, pool acquire timeout   Transient, NOT sent
 *   response timeout, reset, premature close, other IO, call timeout             Transient, AMBIGUOUS
 * Anything else                                                                    Transient, AMBIGUOUS
 * </pre>
 */
public class HttpFailureClassifier {

    private final Set<Integer> permanentStatuses;

    public HttpFailureClassifier(Set<Integer> permanentStatuses) {
        this.permanentStatuses = Set.copyOf(permanentStatuses);
    }

    public FailureException classify(Throwable t, String operation) {
        if (t instanceof FailureException alreadyClassified) {
            return alreadyClassified;
        }
        WebClientResponseException response = Throwables.find(t, WebClientResponseException.class);
        if (response != null) {
            return fromStatus(response.getStatusCode().value(), operation, response);
        }
        if (requestNeverSent(t)) {
            return new TransientFailureException("REQUEST_NOT_SENT", operation + ": " + Throwables.rootMessage(t),
                    null, false, t);
        }
        if (Throwables.find(t, TimeoutException.class) != null
                || Throwables.hasClassNamed(t, "ReadTimeoutException", "PrematureCloseException")
                || Throwables.find(t, IOException.class) != null) {
            return new TransientFailureException("NO_RESPONSE", operation + ": " + Throwables.rootMessage(t), null,
                    true, t);
        }
        return new TransientFailureException("UNEXPECTED_ERROR", operation + ": " + Throwables.rootMessage(t), null,
                true, t);
    }

    public FailureException fromStatus(int status, String operation, Throwable cause) {
        String message = operation + ": HTTP " + status;
        if (status == 401 || status == 403) {
            return new SystemicFailureException(status == 401 ? "UNAUTHORIZED" : "FORBIDDEN", message, status, cause);
        }
        if (status == 408 || status == 429 || status == 503) {
            return new TransientFailureException(status == 429 ? "THROTTLED" : status == 503 ? "UNAVAILABLE"
                    : "REQUEST_TIMEOUT", message, status, false, cause);
        }
        if (permanentStatuses.contains(status) || (status >= 400 && status < 500)) {
            return new PermanentFailureException("CLIENT_ERROR", message + bodySnippet(cause), status, cause);
        }
        if (status == 500) {
            return new ServerErrorFailureException("SERVER_ERROR", message + bodySnippet(cause), status, cause);
        }
        return new TransientFailureException(status >= 500 ? "GATEWAY_ERROR" : "UNEXPECTED_STATUS", message, status,
                true, cause);
    }

    private static boolean requestNeverSent(Throwable t) {
        // Netty's ConnectTimeoutException and AnnotatedConnectException both extend ConnectException.
        return Throwables.find(t, ConnectException.class) != null
                || Throwables.find(t, UnknownHostException.class) != null
                || Throwables.find(t, NoRouteToHostException.class) != null
                || Throwables.find(t, SSLHandshakeException.class) != null
                || Throwables.hasClassNamed(t, "PoolAcquireTimeoutException", "PoolAcquirePendingLimitException");
    }

    private static String bodySnippet(Throwable cause) {
        if (cause instanceof WebClientResponseException e) {
            String body = e.getResponseBodyAsString();
            if (!body.isBlank()) {
                return " body=" + (body.length() > 500 ? body.substring(0, 500) + "..." : body);
            }
        }
        return "";
    }
}
