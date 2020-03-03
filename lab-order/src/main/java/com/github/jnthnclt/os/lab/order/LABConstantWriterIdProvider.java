package com.github.jnthnclt.os.lab.order;

public class LABConstantWriterIdProvider implements LABWriterIdProvider {
    private final int writerId;

    public LABConstantWriterIdProvider(int writerId) {
        this.writerId = writerId;
    }

    public LABWriterId getWriterId() {
        return new LABConstantWriterId(this.writerId);
    }
}
