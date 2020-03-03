package com.github.jnthnclt.os.lab.order;

public interface LABWriterId {
    /**
     * @return the writer ID integer
     */
    int getId();

    /**
     * @return whether or not the writer ID is valid at the time of this call
     */
    boolean isValid();
}
