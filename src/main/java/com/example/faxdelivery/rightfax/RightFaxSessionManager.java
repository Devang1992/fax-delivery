package com.example.faxdelivery.rightfax;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import com.example.faxdelivery.error.SessionExpiredException;
import com.example.faxdelivery.error.SystemicFailureException;

/**
 * rf-session cookie, shared by every call on every listener thread. Held in memory only - never stored,
 * never logged.
 * <ul>
 *   <li>Reused until sessionTtl - refreshBefore (or until a 401 when no TTL is configured).</li>
 *   <li>Single flight: one thread logs in, concurrent callers wait for that result.</li>
 *   <li>401 on a call: drop that exact cookie, log in, repeat the call ONCE; a second 401 is Systemic.</li>
 * </ul>
 */
public class RightFaxSessionManager {

    private final Supplier<String> login;
    private final Duration validFor;
    private final Clock clock;
    private final ReentrantLock lock = new ReentrantLock();
    private volatile Session current;

    private record Session(String cookie, Instant expiresAt) {
    }

    public RightFaxSessionManager(Supplier<String> login, Duration sessionTtl, Duration refreshBefore, Clock clock) {
        this.login = login;
        this.clock = clock;
        if (sessionTtl == null) {
            this.validFor = null;
        }
        else {
            Duration v = sessionTtl.minus(refreshBefore == null ? Duration.ZERO : refreshBefore);
            this.validFor = v.isNegative() || v.isZero() ? sessionTtl : v;
        }
    }

    public <T> T withSession(Function<String, T> call) {
        String cookie = currentCookie();
        try {
            return call.apply(cookie);
        }
        catch (SessionExpiredException expired) {
            invalidate(cookie);
            String fresh = currentCookie();
            try {
                return call.apply(fresh);
            }
            catch (SessionExpiredException again) {
                invalidate(fresh);
                throw new SystemicFailureException("AUTH_REJECTED_AFTER_RELOGIN",
                        "call rejected with 401 right after a fresh login", 401, again);
            }
        }
    }

    String currentCookie() {
        Session s = current;
        if (valid(s)) {
            return s.cookie();
        }
        lock.lock();
        try {
            s = current;
            if (valid(s)) {
                return s.cookie();
            }
            String cookie = login.get();
            current = new Session(cookie, validFor == null ? Instant.MAX : clock.instant().plus(validFor));
            return cookie;
        }
        finally {
            lock.unlock();
        }
    }

    void invalidate(String cookie) {
        lock.lock();
        try {
            if (current != null && current.cookie().equals(cookie)) {
                current = null;
            }
        }
        finally {
            lock.unlock();
        }
    }

    private boolean valid(Session s) {
        return s != null && clock.instant().isBefore(s.expiresAt());
    }
}
