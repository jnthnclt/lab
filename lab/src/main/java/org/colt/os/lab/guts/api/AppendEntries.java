package org.colt.os.lab.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface AppendEntries {

    boolean consume(AppendEntryStream stream) throws Exception;
}
