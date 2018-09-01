package com.github.jnthnclt.os.lab.nn;

import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.List;

public class Util {

    static public List<D_XY> wrap(D_XY[] _points) { // _p is array of XY_I
        if (_points == null) {
            return null;
        }
        if (_points.length < 2) {
            return null;
        }

        // find the extreme point on the hull
        int next = 0;
        for (int i = 1; i < _points.length - 1; i++) {
            if (_points[i].x < _points[next].x || ((_points[i].x == _points[next].x) && (_points[i].y < _points[next].y))) {
                next = i;
            }
        }
        _points[_points.length - 1] = _points[next];

        List<D_XY> result = new ArrayList<>(); // traverse this ordered XY_I[] to draw convex hull (wrap)
        for (int i = 0; i < _points.length - 1; i++) {
            D_XY swap = _points[next];
            _points[next] = _points[i];
            _points[i] = swap;
            next = i + 1;

            result.add(new D_XY(_points[i].x, _points[i].y)); // line from prev point to this point has the minimum polar angle

            for (int j = i + 2; j < _points.length; j++) {
                if (isPointRightOfLine(_points[j], _points[i], _points[next])) {
                    next = j; // j=point; i <-> next = line
                }
            }
            if (next == _points.length - 1) {
                return result;
            }
        }
        return null;
    }

    static public boolean isPointRightOfLine(D_XY p, D_XY p0, D_XY p1) {// p=point; p0 <-> p1 = line
        D_XY a = new D_XY(p1.x - p0.x, p1.y - p0.y);
        D_XY b = new D_XY(p.x - p0.x, p.y - p0.y);
        double sa = a.x * b.y - b.x * a.y;
        if (sa < 0.0) {
            return true;
        }
        return false;
    }


    static List<D_XY> computeCorners(List<D_XY> convexHullPoints) {
        int alignmentPointIndex = computeAlignmentPointIndex(convexHullPoints);
        Rectangle2D r = computeAlignedBounds(convexHullPoints, alignmentPointIndex);

        List<D_XY> alignedCorners = new ArrayList<>();
        alignedCorners.add(new D_XY(r.getMinX(), r.getMinY()));
        alignedCorners.add(new D_XY(r.getMaxX(), r.getMinY()));
        alignedCorners.add(new D_XY(r.getMaxX(), r.getMaxY()));
        alignedCorners.add(new D_XY(r.getMinX(), r.getMaxY()));

        D_XY center = convexHullPoints.get(alignmentPointIndex);
        double angleRad = computeEdgeAngleRad(
            convexHullPoints, alignmentPointIndex);

        AffineTransform at = new AffineTransform();
        at.concatenate(
            AffineTransform.getTranslateInstance(
                center.getX(), center.getY()));
        at.concatenate(
            AffineTransform.getRotateInstance(angleRad));

        List<D_XY> corners = transform(alignedCorners, at);
        return corners;
    }

    private static int computeAlignmentPointIndex(
        List<D_XY> points) {
        double minArea = Double.MAX_VALUE;
        int minAreaIndex = -1;
        for (int i = 0; i < points.size(); i++) {
            Rectangle2D r = computeAlignedBounds(points, i);
            double area = r.getWidth() * r.getHeight();

            if (area < minArea) {
                minArea = area;
                minAreaIndex = i;
            }
        }
        return minAreaIndex;
    }

    private static double computeEdgeAngleRad(
        List<D_XY> points, int index) {
        int i0 = index;
        int i1 = (i0 + 1) % points.size();
        D_XY p0 = points.get(i0);
        D_XY p1 = points.get(i1);
        double dx = p1.getX() - p0.getX();
        double dy = p1.getY() - p0.getY();
        double angleRad = Math.atan2(dy, dx);
        return angleRad;
    }

    private static Rectangle2D computeAlignedBounds(
        List<D_XY> points, int index) {
        D_XY p0 = points.get(index);
        double angleRad = computeEdgeAngleRad(points, index);
        AffineTransform at = createTransform(-angleRad, p0);
        List<D_XY> transformedPoints = transform(points, at);
        Rectangle2D bounds = computeBounds(transformedPoints);
        return bounds;
    }

    private static AffineTransform createTransform(
        double angleRad, D_XY center) {
        AffineTransform at = new AffineTransform();
        at.concatenate(
            AffineTransform.getRotateInstance(angleRad));
        at.concatenate(
            AffineTransform.getTranslateInstance(
                -center.getX(), -center.getY()));
        return at;
    }

    private static List<D_XY> transform(
        List<D_XY> points, AffineTransform at) {
        List<D_XY> result = new ArrayList<>();
        for (D_XY p : points) {
            Point2D tp = at.transform(new Point2D.Double(p.x, p.y), null);
            result.add(new D_XY(tp.getX(), tp.getY()));
        }
        return result;
    }


    private static Rectangle2D computeBounds(
        List<D_XY> points) {
        double minX = Double.MAX_VALUE;
        double minY = Double.MAX_VALUE;
        double maxX = -Double.MAX_VALUE;
        double maxY = -Double.MAX_VALUE;
        for (D_XY p : points) {
            double x = p.getX();
            double y = p.getY();
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x);
            maxY = Math.max(maxY, y);
        }
        return new Rectangle2D.Double(minX, minY, maxX - minX, maxY - minY);
    }


}
