package io.github.mameli;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by mameli on 19/02/2017.
 * Combine partial sums
 */
public class Combine extends Reducer<Center, Point, Center, Point> {

    @Override
    public void reduce(Center key, Iterable<Point> values, Context context)
            throws IOException, InterruptedException {
        Logger logger = Logger.getLogger(Reduce.class);

        Point sumValues = new Point();
        int countValues = 0;
        double xTemp;
        double yTemp;
        for (Point p : values) {
            xTemp = p.getX().get() + sumValues.getX().get();
            sumValues.setX(new DoubleWritable(xTemp));
            yTemp = p.getY().get() + sumValues.getY().get();
            sumValues.setY(new DoubleWritable(yTemp));
            countValues++;
        }
        key.setNumberOfPoints(new IntWritable(countValues));
        logger.error(key.toString() + "\t" + sumValues.toString());
        context.write(new Center(key), sumValues);
    }
}
