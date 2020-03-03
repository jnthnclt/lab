package com.github.jnthnclt.os.lab.order;

public interface LABWriterIdProvider {
    LABWriterId getWriterId() throws LABOutOfWriterIdsException;
}
