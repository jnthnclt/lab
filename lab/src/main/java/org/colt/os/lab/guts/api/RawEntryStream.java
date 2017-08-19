package org.colt.os.lab.guts.api;

import org.colt.os.lab.api.FormatTransformer;
import org.colt.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface RawEntryStream {

    boolean stream(FormatTransformer readKeyFormatTransformer, FormatTransformer readValueFormatTransformer, BolBuffer rawEntry) throws Exception;
}
