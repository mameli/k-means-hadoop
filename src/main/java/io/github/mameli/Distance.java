package io.github.mameli;

import org.apache.hadoop.io.DoubleWritable;

import java.util.List;

/**
 * Created by mameli on 20/02/2017.
 * Find distance between points
 */
class Distance {
    static Double findDistance(Point p1, Point p2) {
        int len = p1.getListOfCoordinates().size();
        List<DoubleWritable> l1 = p1.getListOfCoordinates();
        List<DoubleWritable> l2 = p2.getListOfCoordinates();
        Double sum = 0.0;
        for (int i = 0; i < len; i++) {
            sum += Math.pow(l1.get(i).get() - l2.get(i).get(), 2);
        }
        return Math.sqrt(sum);
    }
}
