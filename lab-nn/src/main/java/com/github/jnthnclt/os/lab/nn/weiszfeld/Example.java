package com.github.jnthnclt.os.lab.nn.weiszfeld;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Example {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException {

        //"tests.txt"
        /*
3

2
3
1 0 0
1 0 4
1 5 0

2
3
1 1 0
1 1 4
1 6 0

2
3
1 1 0
1 1 5
1 6 0
         */

        FileInputStream fis = new FileInputStream("tests.txt");
        Scanner sc = new Scanner(fis);

        /** Reading input **/

        // number of test cases
        int tc = sc.nextInt();
        for (int itc = 0; itc < tc; itc++) {
            // dimension
            int n = sc.nextInt();
            // number of points
            int m = sc.nextInt();
            List<WeightedPoint> wPoints = new ArrayList<>(m);
            // points with respective weights
            for (int i = 0; i < m; i++) {

                double weight = sc.nextDouble();

                double values[] = new double[n];
                for (int j = 0; j < n; j++) {
                    double val = sc.nextDouble();
                    values[j] = val;
                }

                WeightedPoint wPoint = new WeightedPoint(new Point(values), weight);

                wPoints.add(wPoint);
            }


            Input input = new Input();

            input.setDimension(n);
            input.setPoints(wPoints);
            input.setPermissibleError(0.00001);

            WeiszfeldAlgorithm weiszfeld = new WeiszfeldAlgorithm();
            Output output = weiszfeld.process(input);

            Point result = output.getPoint();

            System.out.println(result.toString());
        }

    }
}
