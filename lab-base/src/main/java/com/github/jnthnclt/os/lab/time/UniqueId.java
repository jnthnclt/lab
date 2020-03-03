package com.github.jnthnclt.os.lab.time;

public interface UniqueId {

    /**
     * @return the node ID integer
     */
    int getId();

    /**
     * @return whether or not the writer ID is valid at the time of this call
     */
    boolean isValid();

}
