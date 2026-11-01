package com.example.faxdelivery.error;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.ConnectException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

class HttpFailureClassifierTest {

    private final HttpFailureClassifier classifier = new HttpFailureClassifier(Set.of(400, 404, 409, 422));

    private FailureException status(int code) {
        return classifier.classify(WebClientResponseException.create(code, "x", new HttpHeaders(),
                "body".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8), "OP");
    }

    @Test
    void clientErrorsArePermanent() {
        assertThat(status(400)).isInstanceOf(PermanentFailureException.class);
        assertThat(status(404)).isInstanceOf(PermanentFailureException.class);
        assertThat(status(418)).isInstanceOf(PermanentFailureException.class);
    }

    @Test
    void authErrorsAreSystemic() {
        assertThat(status(401)).isInstanceOf(SystemicFailureException.class);
        assertThat(status(403)).isInstanceOf(SystemicFailureException.class);
    }

    @Test
    void notProcessedStatusesAreTransientAndNotAmbiguous() {
        for (int code : new int[] {408, 429, 503}) {
            FailureException f = status(code);
            assertThat(f).isInstanceOf(TransientFailureException.class);
            assertThat(f.isAmbiguous()).as("status %s", code).isFalse();
        }
    }

    @Test
    void serverErrorsAreAmbiguous() {
        assertThat(status(500)).isInstanceOf(ServerErrorFailureException.class).matches(FailureException::isAmbiguous);
        assertThat(status(502)).isInstanceOf(TransientFailureException.class).matches(FailureException::isAmbiguous);
        assertThat(status(504)).isNotInstanceOf(ServerErrorFailureException.class)
                .matches(FailureException::isAmbiguous);
    }

    @Test
    void connectFailureMeansNotSent() {
        FailureException f = classifier.classify(new WebClientRequestException(new ConnectException("refused"),
                HttpMethod.POST, URI.create("https://x"), new HttpHeaders()), "OP");
        assertThat(f).isInstanceOf(TransientFailureException.class);
        assertThat(f.isAmbiguous()).isFalse();
        assertThat(f.reason()).isEqualTo("REQUEST_NOT_SENT");
    }

    @Test
    void timeoutIsAmbiguous() {
        FailureException f = classifier.classify(new RuntimeException(new TimeoutException("slow")), "OP");
        assertThat(f.isAmbiguous()).isTrue();
        assertThat(f.reason()).isEqualTo("NO_RESPONSE");
    }
}
