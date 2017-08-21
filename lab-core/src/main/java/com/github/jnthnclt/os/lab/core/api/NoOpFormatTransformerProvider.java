package com.github.jnthnclt.os.lab.core.api;

/**
 *
 * @author jonathan.colt
 */
public class NoOpFormatTransformerProvider implements FormatTransformerProvider {

    public static final String NAME = "noOpFormatTransformerProvider";

    public static final NoOpFormatTransformerProvider NO_OP = new NoOpFormatTransformerProvider();

    private NoOpFormatTransformerProvider() {
    }

    @Override
    public FormatTransformer read(long format) throws Exception {
        return FormatTransformer.NO_OP;
    }

    @Override
    public FormatTransformer write(long format) throws Exception {
        return FormatTransformer.NO_OP;
    }
}
