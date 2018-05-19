package com.github.jnthnclt.os.lab.core.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class LABLoggerFactory {

    public interface LABLoggerProvider {
        LABLogger createLogger(String name);
    }

    public static final ConcurrentHashMap<String, LABLogger> loggers = new ConcurrentHashMap();
    public static final AtomicReference<LABLoggerProvider> LAB_LOGGER_PROVIDER = new AtomicReference<>(
        name -> new SysoutLABLogger(name, SysoutLABLoggerLevel.INFO));

    public static LABLogger getLogger() {
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        String name = elements[2].getClassName();
        return loggers.computeIfAbsent(name, s -> LAB_LOGGER_PROVIDER.get().createLogger(name));

    }

    public enum SysoutLABLoggerLevel {
        ERROR, WARN, INFO, DEBUG
    }

    public static class SysoutLABLogger implements LABLogger {

        private final String name;
        private final SysoutLABLoggerLevel level;

        public SysoutLABLogger(String name, SysoutLABLoggerLevel level) {
            this.name = name;
            this.level = level;
        }


        @Override
        public void debug(String msg) {
            if (level.ordinal() >= SysoutLABLoggerLevel.DEBUG.ordinal()) {
                System.out.println("DEBUG: " + Thread.currentThread().getName() + " " + name + " " + msg);
            }
        }

        @Override
        public void debug(String messagePattern, Object arg) {
            if (level.ordinal() >= SysoutLABLoggerLevel.DEBUG.ordinal()) {
                System.out.println("DEBUG: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, arg));
            }
        }

        @Override
        public void debug(String messagePattern, Object arg1, Object arg2) {
            if (level.ordinal() >= SysoutLABLoggerLevel.DEBUG.ordinal()) {
                System.out.println("DEBUG: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, arg1, arg2));
            }
        }

        @Override
        public void debug(String messagePattern, Object arg1, Object arg2, Object arg3) {
            if (level.ordinal() >= SysoutLABLoggerLevel.DEBUG.ordinal()) {
                System.out.println("DEBUG: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, arg1, arg2, arg3));
            }
        }

        @Override
        public void debug(String messagePattern, Object arg1, Object arg2, Object arg3, Object arg4) {
            if (level.ordinal() >= SysoutLABLoggerLevel.DEBUG.ordinal()) {
                System.out.println(
                    "DEBUG: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, arg1, arg2, arg3, arg4));
            }
        }

        @Override
        public void debug(String messagePattern, Object[] argArray, Throwable t) {
            if (level.ordinal() >= SysoutLABLoggerLevel.DEBUG.ordinal()) {
                System.out.println("DEBUG: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, argArray));
            }
        }

        @Override
        public void debug(String messagePattern, Object... argArray) {
            if (level.ordinal() >= SysoutLABLoggerLevel.DEBUG.ordinal()) {
                System.out.println("DEBUG: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, argArray));
            }
        }

        @Override
        public void debug(String msg, Throwable t) {
            if (level.ordinal() >= SysoutLABLoggerLevel.DEBUG.ordinal()) {
                System.out.println("DEBUG: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(msg));
            }
        }

        @Override
        public void warn(String msg) {
            if (level.ordinal() >= SysoutLABLoggerLevel.WARN.ordinal()) {
                System.out.println("WARN: " + Thread.currentThread().getName() + " " + name + " " + msg);
            }
        }

        @Override
        public void warn(String messagePattern, Object arg) {
            if (level.ordinal() >= SysoutLABLoggerLevel.WARN.ordinal()) {
                System.out.println("WARN: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, arg));
            }
        }

        @Override
        public void warn(String messagePattern, Object arg1, Object arg2) {
            if (level.ordinal() >= SysoutLABLoggerLevel.WARN.ordinal()) {
                System.out.println("WARN: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, arg1, arg2));
            }
        }

        @Override
        public void warn(String messagePattern, Object[] argArray, Throwable t) {
            if (level.ordinal() >= SysoutLABLoggerLevel.WARN.ordinal()) {
                System.out.println("WARN: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, argArray));
            }
        }

        @Override
        public void warn(String messagePattern, Object... argArray) {
            if (level.ordinal() >= SysoutLABLoggerLevel.WARN.ordinal()) {
                System.out.println("WARN: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, argArray));
            }
        }

        @Override
        public void warn(String msg, Throwable t) {
            if (level.ordinal() >= SysoutLABLoggerLevel.WARN.ordinal()) {
                System.out.println("WARN: " + Thread.currentThread().getName() + " " + name + " " + msg);
            }
        }

        @Override
        public void info(String msg) {
            if (level.ordinal() >= SysoutLABLoggerLevel.INFO.ordinal()) {
                System.out.println("INFO: " + Thread.currentThread().getName() + " " + name + " " + msg);
            }
        }

        @Override
        public void info(String messagePattern, Object arg) {
            if (level.ordinal() >= SysoutLABLoggerLevel.INFO.ordinal()) {
                System.out.println("INFO: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, arg));
            }
        }

        @Override
        public void info(String messagePattern, Object arg1, Object arg2) {
            if (level.ordinal() >= SysoutLABLoggerLevel.INFO.ordinal()) {
                System.out.println("INFO: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, arg1, arg2));
            }
        }

        @Override
        public void info(String messagePattern, Object[] argArray, Throwable t) {
            if (level.ordinal() >= SysoutLABLoggerLevel.INFO.ordinal()) {
                System.out.println("INFO: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, argArray));
            }
        }

        @Override
        public void info(String messagePattern, Object... argArray) {
            if (level.ordinal() >= SysoutLABLoggerLevel.INFO.ordinal()) {
                System.out.println("INFO: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, argArray));
            }
        }

        @Override
        public void info(String messagePattern, Throwable t) {
            if (level.ordinal() >= SysoutLABLoggerLevel.INFO.ordinal()) {
                System.out.println("INFO: " + Thread.currentThread().getName() + " " + name + " " + messagePattern);
            }
        }

        @Override
        public void error(String msg) {
            if (level.ordinal() >= SysoutLABLoggerLevel.ERROR.ordinal()) {
                System.out.println("ERROR: " + Thread.currentThread().getName() + " " + name + " " + msg);
            }
        }

        @Override
        public void error(String messagePattern, Object arg) {
            if (level.ordinal() >= SysoutLABLoggerLevel.ERROR.ordinal()) {
                System.out.println("ERROR: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, arg));
            }
        }

        @Override
        public void error(String messagePattern, Object arg1, Object arg2) {
            if (level.ordinal() >= SysoutLABLoggerLevel.ERROR.ordinal()) {
                System.out.println("ERROR: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, arg1, arg2));
            }
        }

        @Override
        public void error(String messagePattern, Object[] argArray, Throwable t) {
            if (level.ordinal() >= SysoutLABLoggerLevel.ERROR.ordinal()) {
                System.out.println("ERROR: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, argArray));
            }
        }

        @Override
        public void error(String messagePattern, Object... argArray) {
            if (level.ordinal() >= SysoutLABLoggerLevel.ERROR.ordinal()) {
                System.out.println("ERROR: " + Thread.currentThread().getName() + " " + name + " " + MessageFormatter.format(messagePattern, argArray));
            }
        }

        @Override
        public void error(String messagePattern, Throwable t) {
            if (level.ordinal() >= SysoutLABLoggerLevel.ERROR.ordinal()) {
                System.out.println("ERROR: " + Thread.currentThread().getName() + " " + name + " " + messagePattern);
            }
        }

        @Override
        public void inc(String name) {

        }

        @Override
        public void inc(String name, long amount) {

        }

        @Override
        public void incAtomic(String name) {

        }

        @Override
        public void decAtomic(String name) {

        }

        @Override
        public void set(String s, long value) {

        }
    }
}
