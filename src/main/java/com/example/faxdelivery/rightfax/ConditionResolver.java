package com.example.faxdelivery.rightfax;

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/** Raw RightFax condition -> SUCCESS / FAILURE / PENDING using the configured sets (case-insensitive). */
public class ConditionResolver {

    private final Set<String> success;
    private final Set<String> failure;

    public ConditionResolver(Set<String> success, Set<String> failure) {
        this.success = normalize(success);
        this.failure = normalize(failure);
    }

    public Condition resolve(String raw) {
        if (raw == null || raw.isBlank()) {
            return Condition.PENDING;
        }
        String c = raw.trim().toLowerCase(Locale.ROOT);
        if (success.contains(c)) {
            return Condition.SUCCESS;
        }
        if (failure.contains(c)) {
            return Condition.FAILURE;
        }
        return Condition.PENDING;
    }

    private static Set<String> normalize(Set<String> values) {
        return values.stream().map(v -> v.trim().toLowerCase(Locale.ROOT)).collect(Collectors.toUnmodifiableSet());
    }
}
