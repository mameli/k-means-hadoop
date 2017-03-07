package io.github.mameli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by mameli on 19/02/2017.
 * Combine partial sums
 */
public class Combine extends Reducer<Center, Point, Center, Point> {

    @Override
    public void reduce(Center key, Iterable<Point> values, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        Point sumValues = new Point(conf.getInt("iCoordinates", 2));
        int countValues = 0;
        Double temp;
        for (Point p : values) {
            for (int i = 0; i < p.getListOfCoordinates().size(); i++) {
                temp = sumValues.getListOfCoordinates().get(i).get() + p.getListOfCoordinates().get(i).get();
                sumValues.getListOfCoordinates().get(i).set(temp);
            }
            countValues++;
        }
        key.setNumberOfPoints(new IntWritable(countValues));
        context.write(key, sumValues);
    }
}
