package io.github.mameli;

import org.apache.hadoop.conf.Configuration;
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
        Configuration conf = context.getConfiguration();

        Point sumValues = new Point(conf.getInt("iParameters", 2));
        int countValues = 0;
        Double temp;
        for (Point p : values) {
            for (int i = 0; i < p.getListOfParameters().size(); i++) {
                temp = sumValues.getListOfParameters().get(i).get() + p.getListOfParameters().get(i).get();
                sumValues.getListOfParameters().get(i).set(temp);
            }
            countValues++;
        }
        key.setNumberOfPoints(new IntWritable(countValues));
        logger.fatal(key.toString() + " \tSum:" + sumValues.toString());
        context.write(new Center(key), sumValues);
    }
}
