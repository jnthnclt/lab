package org.colt.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface FormatTransformerProvider {

    FormatTransformer read(long format) throws Exception;

    FormatTransformer write(long format) throws Exception;

}
