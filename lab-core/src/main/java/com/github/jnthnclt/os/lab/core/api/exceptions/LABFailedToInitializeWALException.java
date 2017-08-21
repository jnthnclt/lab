package com.github.jnthnclt.os.lab.core.api.exceptions;

/**
 *
 * @author jonathan.colt
 */
public class LABFailedToInitializeWALException extends Exception {

    public LABFailedToInitializeWALException(String string, Exception x) {
        super(string, x);
    }

}
