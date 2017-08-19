package org.colt.os.lab.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class LABLoggerFactory {

    public interface LABLoggerProvider {
        org.colt.os.lab.util.LABLogger createLogger(String name);
    }

    public static final ConcurrentHashMap<String, org.colt.os.lab.util.LABLogger> loggers = new ConcurrentHashMap();
    public static final AtomicReference<LABLoggerProvider> LAB_LOGGER_PROVIDER = new AtomicReference<>(name -> {
        return new SysoutLABLogger(name);
    });

    public static org.colt.os.lab.util.LABLogger getLogger() {
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        String name = elements[2].getClassName();
        return loggers.computeIfAbsent(name, s -> {
            return LAB_LOGGER_PROVIDER.get().createLogger(name);
        });

    }

    public static class SysoutLABLogger implements LABLogger {

        private final String name;

        public SysoutLABLogger(String name) {
            this.name = name;
        }


        @Override
        public void debug(String msg) {
            System.out.println(Thread.currentThread().getName()+" "+name+" "+msg);
        }

        @Override
        public void debug(String messagePattern, Object arg) {

        }

        @Override
        public void debug(String messagePattern, Object arg1, Object arg2) {

        }

        @Override
        public void debug(String messagePattern, Object arg1, Object arg2, Object arg3) {

        }

        @Override
        public void debug(String messagePattern, Object arg1, Object arg2, Object arg3, Object arg4) {

        }

        @Override
        public void debug(String messagePattern, Object[] argArray, Throwable t) {

        }

        @Override
        public void debug(String messagePattern, Object... argArray) {

        }

        @Override
        public void debug(String msg, Throwable t) {

        }

        @Override
        public void warn(String msg) {

        }

        @Override
        public void warn(String messagePattern, Object arg) {

        }

        @Override
        public void warn(String messagePattern, Object arg1, Object arg2) {

        }

        @Override
        public void warn(String messagePattern, Object[] argArray, Throwable t) {

        }

        @Override
        public void warn(String messagePattern, Object... argArray) {

        }

        @Override
        public void warn(String msg, Throwable t) {

        }

        @Override
        public void info(String msg) {

        }

        @Override
        public void info(String messagePattern, Object arg) {

        }

        @Override
        public void info(String messagePattern, Object arg1, Object arg2) {

        }

        @Override
        public void info(String messagePattern, Object[] argArray, Throwable t) {

        }

        @Override
        public void info(String messagePattern, Object... argArray) {

        }

        @Override
        public void info(String messagePattern, Throwable t) {

        }

        @Override
        public void error(String msg) {

        }

        @Override
        public void error(String messagePattern, Object arg) {

        }

        @Override
        public void error(String messagePattern, Object arg1, Object arg2) {

        }

        @Override
        public void error(String messagePattern, Object[] argArray, Throwable t) {

        }

        @Override
        public void error(String messagePattern, Object... argArray) {

        }

        @Override
        public void error(String messagePattern, Throwable t) {

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
