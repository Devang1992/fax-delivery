package com.example.faxdelivery.rightfax;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.example.faxdelivery.error.SessionExpiredException;
import com.example.faxdelivery.error.SystemicFailureException;

class RightFaxSessionManagerTest {

    private final AtomicInteger logins = new AtomicInteger();
    private final RightFaxSessionManager sessions = new RightFaxSessionManager(
            () -> "cookie-" + logins.incrementAndGet(), null, null, Clock.systemUTC());

    @Test
    void reusesTheCookie() {
        sessions.withSession(c -> c);
        sessions.withSession(c -> c);
        assertThat(logins.get()).isEqualTo(1);
    }

    @Test
    void reLogsInOnceOn401AndRepeatsTheCall() {
        AtomicInteger calls = new AtomicInteger();
        String result = sessions.withSession(c -> {
            if (calls.incrementAndGet() == 1) {
                throw new SessionExpiredException("OP", null);
            }
            return c;
        });
        assertThat(result).isEqualTo("cookie-2");
        assertThat(logins.get()).isEqualTo(2);
    }

    @Test
    void secondConsecutive401IsSystemic() {
        assertThatThrownBy(() -> sessions.withSession(c -> {
            throw new SessionExpiredException("OP", null);
        })).isInstanceOf(SystemicFailureException.class).hasMessageContaining("fresh login");
    }

    @Test
    void ttlForcesARelogin() {
        RightFaxSessionManager shortLived = new RightFaxSessionManager(() -> "c" + logins.incrementAndGet(),
                Duration.ofMillis(1), Duration.ZERO, Clock.systemUTC());
        shortLived.withSession(c -> c);
        try {
            Thread.sleep(5);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        shortLived.withSession(c -> c);
        assertThat(logins.get()).isEqualTo(2);
    }
}
