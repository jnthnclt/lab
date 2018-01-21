package com.github.jnthnclt.os.lab.collections.oh;

/**
 * @author jonathan.colt
 */
public interface OHasher<K> {

    OHasher SINGLETON = key -> key.hashCode();

    int hashCode(K key);
}
