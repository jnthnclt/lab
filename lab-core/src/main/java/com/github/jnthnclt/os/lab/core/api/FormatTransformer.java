package com.github.jnthnclt.os.lab.core.api;

import com.github.jnthnclt.os.lab.core.io.BolBuffer;

/**
 * @author jonathan.colt
 */
public interface FormatTransformer {

    FormatTransformer NO_OP = new FormatTransformer() {
        @Override
        public BolBuffer transform(BolBuffer bytes) {
            return bytes;
        }

        @Override
        public BolBuffer[] transform(BolBuffer[] bytes) {
            return bytes;
        }
    };

    BolBuffer transform(BolBuffer bytes);

    BolBuffer[] transform(BolBuffer[] bytes);
}
