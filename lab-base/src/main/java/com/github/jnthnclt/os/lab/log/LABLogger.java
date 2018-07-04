package com.github.jnthnclt.os.lab.log;

public interface LABLogger {

    void debug(String msg);

    void debug(String messagePattern, Object arg);

    void debug(String messagePattern, Object arg1, Object arg2);

    void debug(String messagePattern, Object arg1, Object arg2, Object arg3);

    void debug(String messagePattern, Object arg1, Object arg2, Object arg3, Object arg4);

    void debug(String messagePattern, Object[] argArray, Throwable t);

    void debug(String messagePattern, Object... argArray);

    void debug(String msg, Throwable t);

    void warn(String msg);

    void warn(String messagePattern, Object arg);

    void warn(String messagePattern, Object arg1, Object arg2);

    void warn(String messagePattern, Object[] argArray, Throwable t);

    void warn(String messagePattern, Object... argArray);

    void warn(String msg, Throwable t);

    void info(String msg);

    void info(String messagePattern, Object arg);

    void info(String messagePattern, Object arg1, Object arg2);

    void info(String messagePattern, Object[] argArray, Throwable t);

    void info(String messagePattern, Object... argArray);

    void info(String messagePattern, Throwable t);

    void error(String msg);

    void error(String messagePattern, Object arg);

    void error(String messagePattern, Object arg1, Object arg2);

    void error(String messagePattern, Object[] argArray, Throwable t);

    void error(String messagePattern, Object... argArray);

    void error(String messagePattern, Throwable t);

    void inc(String name);

    void inc(String name, long amount);

    void incAtomic(String name);

    void decAtomic(String name);

    void set(String s, long value);
}

