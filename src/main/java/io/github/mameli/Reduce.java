package io.github.mameli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mameli on 19/02/2017.
 * <p>
 * k means reducer
 */
public class Reduce extends Reducer<Center, Point, IntWritable, Center> {

    private List<Center> centers = new ArrayList<Center>();
    private int iConvergedCenters = 0;

    public enum CONVERGE_COUNTER {
        CONVERGED
    }

    @Override
    public void reduce(Center key, Iterable<Point> values, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Logger logger = Logger.getLogger(Reduce.class);
        logger.fatal("Reducer ");
        logger.fatal("center: " + key.toString());
        int numElements = 0;
        Double temp;
        Center newCenter = new Center(conf.getInt("iParameters", 2));
        for (Point p : values) {
            for (int i = 0; i < p.getListOfParameters().size(); i++) {
                temp = newCenter.getListOfParameters().get(i).get() + p.getListOfParameters().get(i).get();
                newCenter.getListOfParameters().get(i).set(temp);
            }
            numElements += key.getNumberOfPoints().get();
        }
        newCenter.divideParameters(numElements);
        newCenter.setIndex(key.getIndex());
        newCenter.setNumberOfPoints(new IntWritable(0));

        if (key.isConverged(newCenter, conf.getDouble("threshold", 0.5)))
            iConvergedCenters++;

        centers.add(newCenter);
        logger.fatal("New center: " + newCenter.toString());
        context.write(newCenter.getIndex(), newCenter);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Configuration conf = context.getConfiguration();
        Path centersPath = new Path(conf.get("centersFilePath"));
        FileSystem fs = FileSystem.get(conf);
        fs.delete(centersPath, true);
        SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(centersPath),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Center.class));
        for (Center c : centers) {
            centerWriter.append(c.getIndex(), c);
        }
        if (iConvergedCenters == centers.size())
            context.getCounter(CONVERGE_COUNTER.CONVERGED).increment(1);
        centerWriter.close();
    }
}
