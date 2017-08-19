package org.colt.os.lab.api.exceptions;

/**
 *
 * @author jonathan.colt
 */
public class LABCorruptedException extends Exception {

    public LABCorruptedException() {
    }

    public LABCorruptedException(String message) {
        super(message);
    }

    public LABCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
