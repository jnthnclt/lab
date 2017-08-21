package com.github.jnthnclt.os.lab.core.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface AppendEntries {

    boolean consume(AppendEntryStream stream) throws Exception;
}
