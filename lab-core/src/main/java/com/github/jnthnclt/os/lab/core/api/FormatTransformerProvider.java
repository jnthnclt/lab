package com.github.jnthnclt.os.lab.core.api;

/**
 *
 * @author jonathan.colt
 */
public interface FormatTransformerProvider {

    FormatTransformer read(long format) throws Exception;

    FormatTransformer write(long format) throws Exception;

}
