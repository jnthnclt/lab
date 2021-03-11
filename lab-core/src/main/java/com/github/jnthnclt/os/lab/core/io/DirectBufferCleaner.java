package com.github.jnthnclt.os.lab.core.io;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 *
 */
class DirectBufferCleaner {

    private static final Class<?> directBufferClass;
    private static final Method directBufferCleanerMethod;
    private static final Class<?> cleanerClass;
    private static final Method cleanMethod;
    private static final boolean available;

    static {
        Class<?> _directBufferClass = null;
        Method _directBufferCleanerMethod = null;
        Class<?> _cleanerClass = null;
        Method _cleanMethod = null;
        boolean _available = false;
        try {
            _directBufferClass = Class.forName("sun.nio.ch.DirectBuffer");
            _directBufferCleanerMethod = _directBufferClass.getMethod("cleaner");
            _cleanerClass = _directBufferCleanerMethod.getReturnType();
            _cleanMethod = _cleanerClass.getMethod("clean");
            _available = true;
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException e) {
            System.out.println("Failed to reflect direct buffer cleaner, these methods will be unavailable. " +
                               "If you are on Java 9+ add --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED " +
                               "to JVM options");
        }
        directBufferClass = _directBufferClass;
        directBufferCleanerMethod = _directBufferCleanerMethod;
        cleanerClass = _cleanerClass;
        cleanMethod = _cleanMethod;
        available = _available;
    }

    static public void clean(ByteBuffer bb) {
        if (available && cleanMethod != null && directBufferClass.isAssignableFrom(bb.getClass())) {
            try {
                Object cleaner = directBufferCleanerMethod.invoke(bb);
                if (cleaner != null) {
                    cleanMethod.invoke(cleaner);
                }
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                System.out.println("Failed to clean buffer. " +
                                   "If you are on Java 9+ add --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED " +
                                   "to JVM options");
            }
        }
    }
}
