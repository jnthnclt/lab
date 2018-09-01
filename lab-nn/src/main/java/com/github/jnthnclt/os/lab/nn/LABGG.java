package com.github.jnthnclt.os.lab.nn;

import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;

public class LABGG {


    public static void main(String[] args) throws IOException {


        System.out.println((1.0 / 128.0) * 3);

        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);

        int numFeatures = 2;
        int numN = 4096;

        P[] points = new P[numN];

        double[][][] bounds = new double[][][] {
            { { 0, 0.25 }, { 0, 0.25 } },
            { { 0.75, 1 }, { 0.75, 1 } },
            { { 0.6, 0.75 }, { 0.1, 0.2 } },
            { { 0.1, 0.4 }, { 0.6, 0.75 } },

            { { 0, 0.1 }, { 0.9, 1 } },
            { { 0.15, 0.4 }, { 0.8, 0.9 } },
            { { 0.45, 0.9 }, { 0.7, 0.8 } },

        };


        for (int i = 0; i < numN; i++) {

            int bi = i % bounds.length;

            P n = P.random(rand, numFeatures, bounds[bi]);
            points[i] = n;
        }
        //BufferedImage hugeImage = null;

        BufferedImage hugeImage = ImageIO.read(new File("/thredup/red-dress.png"));
        int[][] result = Image.convertTo2DWithoutUsingGetRGB(hugeImage);
//        int iwidth = 128;
//        int iheight = 128;
        int iwidth = hugeImage.getWidth();
        int iheight = hugeImage.getHeight();

        System.out.println(iwidth + " " + iheight);

        numFeatures = 5;
        points = new P[iwidth * iheight];

        int pi = 0;
        for (int y = 0; y < iheight; y++) {
            double yp = y / (double) (iheight * 1);
            for (int x = 0; x < iwidth; x++) {
                double xp = x / (double) (iwidth * 1);

                Color c = new Color(result[y][x]);
                //float[] hsb = Color.RGBtoHSB(c.getRed(), c.getGreen(), c.getBlue(), null);
                //double cd = Double.longBitsToDouble(result[y][x]);

                P n = new P(new double[] { xp, yp, (int) c.getRed(), (int) c.getGreen(), (int) c.getBlue() });
                points[pi] = n;
                //image.setRGB(x, y, result[x][y]);
                pi++;
            }
        }


        System.out.println("Building Agglomerative Graph");
        Distance pixelDistance = (a, b) -> {
            double a1 = a[0] - b[0];
            double a2 = a[1] - b[1];
            if (Math.abs(a1) > 0.0234375 || Math.abs(a2) > 0.0234375) {
                return -1;
            }

            double ed = NN.comparableEuclidianDistance(new double[] { a[0], a[1] }, new double[] { b[0], b[1] });
            ed = Math.sqrt(ed);

            int ac = (Byte.MAX_VALUE << 24) + (((int) a[2] & 0xFF) << 16) + (((int) a[3] & 0xFF) << 8) + ((int) a[4] & 0xFF);
            int bc = (Byte.MAX_VALUE << 24) + (((int) b[2] & 0xFF) << 16) + (((int) b[3] & 0xFF) << 8) + ((int) b[4] & 0xFF);
            double colorDifference = Colors.getColorDifference(ac, bc);
            return (1d - ed) + Math.max(1, Math.abs(colorDifference));

        };

        Distance ed = (a, b) -> NN.comparableEuclidianDistance(a, b);

        Distance distance = pixelDistance;

        NP start = Agglomerative.build(points, distance);
        System.out.println("Built Agglomerative Graph");

        long naiveElapse = 0;
        long fancyElapse = 0;

        AtomicLong naiveCount = new AtomicLong();
        AtomicLong fancyCount = new AtomicLong();

        P naiveFound = null;
        NP fancyFound = null;
        P query = null;

        for (int i = 0; i < 0; i++) {


            query = P.random(rand, numFeatures, null);
            long timestamp = System.currentTimeMillis();
            naiveFound = BruteForce.bruteForce(query, points, distance);
            naiveCount.addAndGet(points.length);
            naiveElapse += System.currentTimeMillis() - timestamp;

            timestamp = System.currentTimeMillis();

            NP np = start;
            fancyFound = null;
            do {
                if (fancyFound != null) {
                    np = fancyFound;
                }
                fancyFound = np.find(query, fancyCount, distance);
                if (fancyFound != null) {
                    //System.out.println(start.n.id + "->" + fancyFound.n.id + ";");
                }
            }
            while (fancyFound != np);
            fancyElapse += System.currentTimeMillis() - timestamp;

        }

//        System.out.println(
//            "fancy:" + fancyCount.get() + " " + fancyElapse + " | " + ed.distance(fancyFound.p.features,
//                query.features) + " answer:" + fancyFound.p);
//        System.out.println(
//            "naive:" + naiveCount.get() + " " + naiveElapse + " | " + ed.distance(naiveFound.features,
//                query.features) + " answer:" + naiveFound);
//        System.out.println("query:" + query);
//
//        System.out.println("seed:" + seed);


        int width = 725;
        int height = 725;

        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);


        Graphics2D g2 = (Graphics2D) image.getGraphics();
        RenderingHints rh = new RenderingHints(
            RenderingHints.KEY_ANTIALIASING,
            RenderingHints.VALUE_ANTIALIAS_ON);
        g2.setRenderingHints(rh);

        if (hugeImage != null) {
            //g2.drawImage(hugeImage, 0, 0, width, height, null);
        }


        g2.setColor(Color.gray);

        System.out.println(start.depth);

        int maxDepth = start.depth;

//        Color[] depthColor = new Color[] { Color.white, Color.red, Color.green, Color.BLUE, Color.MAGENTA,
//            Color.white, Color.red, Color.green, Color.BLUE, Color.MAGENTA };
//        int[] depths = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };


        Color[] depthColor = new Color[maxDepth];
        int[] depths = new int[maxDepth];
        for (int i = 0; i < maxDepth; i++) {
            int gray = (int) (255.0 * (i / (double) maxDepth));

            depthColor[i] = new Color(gray, gray, gray, 25);
            depths[i] = i;
        }

        int minDepth = 1;
        int wrapMaxDepth = 2;

//        start.visit(start.p, (f, depth, t) -> {
//
//            if (depth > minDepth) {
//                if (depth < depthColor.length) {
//                    g2.setColor(depthColor[depth]);
//                } else {
//                    g2.setColor(Color.pink);
//                }
//
//                int fx = (int) (width * f.features[0]);
//                int fy = (int) (height * f.features[1]);
//
//                int tx = (int) (width * t.features[0]);
//                int ty = (int) (height * t.features[1]);
//                g2.drawLine(fx, fy, tx, ty);
//
//                g2.drawOval(fx - depth, fy - depth, depth * 2, depth * 2);
//            }
//        });


//        Color[] depthColor = new Color[] { new Color(255, 0, 0, 225) };
//        int[] depths = new int[] { 4 };

        //for (int i = depths.length-1; i > -1; i--) {


        int alpha = Math.min(255, (int) ((255 - 25) / (double) ((wrapMaxDepth - minDepth))+1));

        System.out.println("Alpha:" + alpha);

        for (int i = Math.min(wrapMaxDepth, depths.length) - 1; i >= minDepth; i--) {
        //for (int i = minDepth; i < Math.min(wrapMaxDepth, depths.length); i++) {
            int di = i;
            ArrayList<D_XY> ps = new ArrayList<>();
            start.leafForDepth(depths[di], (depth, l) -> {

                if (depth != 0) {
                    if (ps.size() > 2) {

                        List<D_XY> wrap = Util.wrap(ps.toArray(new D_XY[0]));
                        if (wrap != null) {

                            wrap = Util.computeCorners(wrap);

                            int[] xs = new int[wrap.size()];
                            int[] ys = new int[wrap.size()];

                            int wi = 0;
                            for (D_XY xy : wrap) {
                                xs[wi] = (int) xy.x;
                                ys[wi] = (int) xy.y;
                                wi++;
                            }

                            g2.setColor(new Color((int) l.features[2], (int) l.features[3], (int) l.features[4], alpha));
                            //g2.setColor(depthColor[di]);
                            g2.fillPolygon(xs, ys, xs.length);

//                        g2.setColor(depthColor[di].brighter());
//                        g2.fillPolygon(xs, ys, xs.length);
//
//                        g2.setColor(Color.darkGray);
//                        g2.drawPolygon(xs, ys, xs.length);
                        }
                    }
                    ps.clear();
                } else if (depth == 0) {
                    int tx = (int) (width * l.features[0]);
                    int ty = (int) (height * l.features[1]);
                    ps.add(new D_XY(tx, ty));
                }

            });
        }


//        start.leafs(l -> {
//            image.setRGB((int) (l.features[0]), (int) (l.features[1]), Color.white.getRGB());
//
//        });

//        for (int i = 0; i < points.length; i++) {
//            image.setRGB((int) (width * points[i].features[0]), (int) (width * points[i].features[1]), Color.white.getRGB());
//        }


        System.out.println("Done");
        JFrame frame = new JFrame();
        frame.getContentPane().setLayout(new FlowLayout());
        frame.getContentPane().add(new JLabel(new ImageIcon(image)));
        frame.pack();
        frame.setVisible(true);

    }


}
