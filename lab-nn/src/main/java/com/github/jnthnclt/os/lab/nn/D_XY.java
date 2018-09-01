package com.github.jnthnclt.os.lab.nn;

public class D_XY {

    public final double x;
    public final double y;

    public D_XY(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof D_XY) {


            if (equal(x, ((D_XY) obj).getX()) && equal(y, ((D_XY) obj).getY())) {
                return true;
            } else{
                return false;
            }
        } else {
            return false;
        }
    }

    boolean equal(double a, double b) {
        return Double.doubleToLongBits(a) == Double.doubleToLongBits(b);
    }

    @Override
    public int hashCode() {
        // http://stackoverflow.com/questions/22826326/good-hashcode-function-for-2d-coordinates
        // http://www.cs.upc.edu/~alvarez/calculabilitat/enumerabilitat.pdf
        int tmp = (int) (y + ((x + 1) / 2));
        return Math.abs((int) (x + (tmp * tmp)));
    }
}
