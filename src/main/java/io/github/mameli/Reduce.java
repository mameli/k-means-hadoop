package io.github.mameli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by mameli on 19/02/2017.
 * k means reducer
 */
public class Reduce extends Reducer<Center, Point, IntWritable, Center> {
    private Logger logger = Logger.getLogger(Reduce.class);
    private HashMap<IntWritable, Center> newCenters = new HashMap<IntWritable, Center>();
    private HashMap<IntWritable, Center> oldCenters = new HashMap<IntWritable, Center>();
    private int iConvergedCenters = 0;

    public enum CONVERGE_COUNTER {
        CONVERGED
    }

    @Override
    public void reduce(Center key, Iterable<Point> values, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        logger.fatal("Reducer ");
        logger.fatal("center: " + key.toString());
        Center newCenter = new Center(conf.getInt("iParameters", 2));
        boolean flagOld = false;
        if (newCenters.containsKey(key.getIndex())) {
            newCenter = newCenters.get(key.getIndex());
            flagOld = true;
        }

        int numElements = 0;
        Double temp;
        for (Point p : values) {
            for (int i = 0; i < p.getListOfParameters().size(); i++) {
                temp = newCenter.getListOfParameters().get(i).get() + p.getListOfParameters().get(i).get();
                newCenter.getListOfParameters().get(i).set(temp);
            }
            numElements += key.getNumberOfPoints().get();
        }
        newCenter.setIndex(key.getIndex());
        newCenter.addNumberOfPoints(new IntWritable(numElements));

        if (!flagOld) {
            newCenters.put(newCenter.getIndex(), newCenter);
            oldCenters.put(key.getIndex(), new Center(key));
        }

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
        Iterator<Center> it = newCenters.values().iterator();
        Center temp;
        Double avgValue = 0.0;
        Double threshold = conf.getDouble("threshold", 0.5);
        int k = conf.getInt("k", 2);
        while (it.hasNext()) {
            temp = it.next();
            temp.divideParameters();
            Center sameIndexC = oldCenters.get(temp.getIndex());
            if (temp.isConverged(sameIndexC, threshold))
                iConvergedCenters++;
            avgValue += Math.pow(Distance.findDistance(temp, sameIndexC), 2);
            centerWriter.append(temp.getIndex(), temp);
        }
        avgValue = Math.sqrt(avgValue / k);
        System.out.println("standard deviation " + avgValue);
        if (iConvergedCenters == newCenters.size() || avgValue < threshold)
            context.getCounter(CONVERGE_COUNTER.CONVERGED).increment(1);
        centerWriter.close();
        logger.fatal("Converged " + iConvergedCenters);
    }

}
