package com.example.faxdelivery.error;

import java.util.IdentityHashMap;
import java.util.Map;

/** Cause-chain helpers (Spring wraps listener exceptions, so classification always walks the chain). */
public final class Throwables {

    private Throwables() {
    }

    public static <T extends Throwable> T find(Throwable t, Class<T> type) {
        Map<Throwable, Boolean> seen = new IdentityHashMap<>();
        for (Throwable c = t; c != null && seen.put(c, Boolean.TRUE) == null; c = c.getCause()) {
            if (type.isInstance(c)) {
                return type.cast(c);
            }
        }
        return null;
    }

    public static boolean hasClassNamed(Throwable t, String... simpleNames) {
        Map<Throwable, Boolean> seen = new IdentityHashMap<>();
        for (Throwable c = t; c != null && seen.put(c, Boolean.TRUE) == null; c = c.getCause()) {
            for (String name : simpleNames) {
                if (c.getClass().getSimpleName().equals(name)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static String rootMessage(Throwable t) {
        Throwable root = t;
        Map<Throwable, Boolean> seen = new IdentityHashMap<>();
        while (root.getCause() != null && seen.put(root, Boolean.TRUE) == null) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }
}
