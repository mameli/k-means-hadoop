package io.github.mameli;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by mameli on 19/02/2017.
 * <p>
 * k means reducer
 */
public class Reduce extends Reducer<Center, Point, Center, Point> {

    @Override
    public void reduce(Center key, Iterable<Point> values, Context context)
            throws IOException, InterruptedException {
        Logger logger = Logger.getLogger(Reduce.class);
        logger.error("Reducer ");
        logger.error("center: " + key.toString());
        int numElements = 0;
        Double sumX = 0.0;
        Double sumY = 0.0;
        for (Point p : values) {
            sumX += p.getX().get();
            sumY += p.getY().get();
            numElements += key.getNumberOfPoints().get();
        }
        logger.error(numElements);
        logger.error("x: " + (sumX / numElements) + "\t y:" + (sumY / numElements));

        context.write(new Center(new DoubleWritable(sumX / numElements), new DoubleWritable(sumY / numElements)), new Point());

    }
}
