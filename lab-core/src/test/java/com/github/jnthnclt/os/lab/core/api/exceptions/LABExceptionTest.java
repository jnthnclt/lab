package com.github.jnthnclt.os.lab.core.api.exceptions;

import org.testng.annotations.Test;

public class LABExceptionTest {

    @Test
    public void sillyConstructorTestForCodeCoverage() {
        new LABClosedException("Foo");
        new LABConcurrentSplitException();
        new LABCorruptedException();
        new LABCorruptedException("Foo");
        new LABCorruptedException("Foo", new RuntimeException());
        new LABFailedToInitializeWALException("foo", new RuntimeException());
    }

}