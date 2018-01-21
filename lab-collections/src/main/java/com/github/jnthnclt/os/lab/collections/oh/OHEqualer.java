package com.github.jnthnclt.os.lab.collections.oh;

/**
 *
 * @author jonathan.colt
 */
public interface OHEqualer<K> {

    OHEqualer SINGLETON = (a,b) -> a.equals(b);

    boolean equals(K a, K b);

}
