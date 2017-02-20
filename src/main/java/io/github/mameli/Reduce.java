package io.github.mameli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
        Center newCenter = new Center(new DoubleWritable(sumX / numElements),
                new DoubleWritable(sumY / numElements),
                new IntWritable(key.getIndex().get()),
                new IntWritable(0));

        if (key.isConverged(newCenter, conf.getDouble("threshold", 0.5)))
            iConvergedCenters++;

        centers.add(newCenter);
        logger.error("New center: " + newCenter.toString());
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
