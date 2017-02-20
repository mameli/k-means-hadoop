package io.github.mameli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by mameli on 19/02/2017.
 * <p>
 * K means mapper
 */

public class Map extends Mapper<Object, Text, Center, Point> {
    private Logger logger = Logger.getLogger(Map.class);
    private List<Center> centers = new ArrayList<Center>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        logger.error("Centers stored in " + conf.get("centersFilePath"));
        Path centersPath = new Path(conf.get("centersFilePath"));
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centersPath));
        IntWritable key = new IntWritable();
        Center value = new Center();
        while (reader.next(key, value)) {
            Center c = new Center(value.getX(), value.getY(), new IntWritable(key.get()), new IntWritable(0));
            logger.info(c.getIndex());
            centers.add(c);
        }
        reader.close();
        logger.error("Centri: " + centers.toString());
        logger.error("Setup end");
        logger.error("Points in file:");
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        List<Double> spaceValues = new ArrayList<Double>();
        StringTokenizer tokenizer = new StringTokenizer(line, ",");
        while (tokenizer.hasMoreTokens()) {
            spaceValues.add(Double.parseDouble(tokenizer.nextToken()));
        }
        Point p = new Point(new DoubleWritable(spaceValues.get(0)), new DoubleWritable(spaceValues.get(1)));

        Center minDistanceCenter = null;
        Double minDistance = Double.MAX_VALUE;
        Double distanceTemp;
        Double tempX;
        Double tempY;
        for (Center c : centers) {
            tempX = Math.pow(p.getX().get() - c.getX().get(), 2);
            tempY = Math.pow(p.getY().get() - c.getY().get(), 2);
            distanceTemp = Math.sqrt(tempX + tempY);
            if (minDistance > distanceTemp) {
                minDistanceCenter = c;
                minDistance = distanceTemp;
            }
        }
        if (minDistanceCenter != null)
            logger.error("P:" + p.toString() + " C: " + minDistanceCenter.getIndex() + " " + minDistanceCenter.toString());
        context.write(new Center(minDistanceCenter), p);
    }


}