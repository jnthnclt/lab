package org.colt.os.lab.api;

import org.colt.os.lab.io.BolBuffer;

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
