package com.github.jnthnclt.os.lab.nn.weiszfeld;

public class WeightedPoint {
    private final Point point;
    private final double weight;

    public WeightedPoint(Point point, double weight) {
        this.point = point;
        this.weight = weight;
    }


    public Point getPoint() {
        return point;
    }


    public double getWeight() {
        return weight;
    }

}
