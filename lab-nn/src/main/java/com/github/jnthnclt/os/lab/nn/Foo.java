package com.github.jnthnclt.os.lab.nn;

public class Foo {

    public static void main(String[] args) {

        double value = 17.73;
        int hangers = 1000;
        double allSellInDays = 32.4 * 2;

        double[] x = hangerScore(value, hangers, allSellInDays);
        System.out.println(x[0] * hangers + " vol:" + x[1]);


        value = 17.32;
        hangers = 1000;
        allSellInDays = 32.6 * 2;


        x = hangerScore(value, hangers, allSellInDays);
        System.out.println(x[0] * hangers + " vol:" + x[1]);


        value = 9.00;  // 8 + 4 + 17 = $29 -> $20  translates to 32 - 19.5 days to sell?
        hangers = 1000;
        allSellInDays = 19.52 * 2;


        x = hangerScore(value, hangers, allSellInDays);
        System.out.println(x[0] * hangers + " vol:" + x[1]);

    }

    private static double[] hangerScore(double value, int hangers, double allSellInDays) {
        double sellPerDay = hangers / allSellInDays;

        double dayStep = (allSellInDays / sellPerDay);

        double c = 0;
        double total = 0;
        for (double i = dayStep; i <= allSellInDays; i += dayStep) {
            total += value / i;
            c++;
        }

        return new double[] { total / c, sellPerDay };
    }
}
