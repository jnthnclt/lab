package com.github.jnthnclt.os.lab.order;

/**
 * @author jcolt
 */
public class LABConstantWriterId implements LABWriterId {

    private final int id;

    public LABConstantWriterId(int id) {
        this.id = id;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public boolean isValid() {
        return true;
    }
}
