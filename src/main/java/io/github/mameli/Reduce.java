package io.github.mameli;

import org.apache.hadoop.conf.Configuration;
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

        Center newCenter = new Center(conf.getInt("iCoordinates", 2));
        boolean flagOld = false;
        if (newCenters.containsKey(key.getIndex())) {
            newCenter = newCenters.get(key.getIndex());
            flagOld = true;
        }

        int numElements = 0;
        Double temp;
        for (Point p : values) {
            for (int i = 0; i < p.getListOfCoordinates().size(); i++) {
                temp = newCenter.getListOfCoordinates().get(i).get() + p.getListOfCoordinates().get(i).get();
                newCenter.getListOfCoordinates().get(i).set(temp);
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
        Configuration conf = context.getConfiguration();
        Path centersPath = new Path(conf.get("centersFilePath"));
        SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(centersPath),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Center.class));
        Iterator<Center> it = newCenters.values().iterator();
        Center newCenterValue;
        Center sameIndexC;
        Double avgValue = 0.0;
        Double threshold = conf.getDouble("threshold", 0.5);
        int k = conf.getInt("k", 2);
        while (it.hasNext()) {
            newCenterValue = it.next();
            newCenterValue.divideCoordinates();
            sameIndexC = oldCenters.get(newCenterValue.getIndex());
            if (newCenterValue.isConverged(sameIndexC, threshold))
                iConvergedCenters++;
            avgValue += Math.pow(Distance.findDistance(newCenterValue, sameIndexC), 2);
            centerWriter.append(newCenterValue.getIndex(), newCenterValue);
        }
        avgValue = Math.sqrt(avgValue / k);
        logger.fatal("Convergence value: " + avgValue);
        int percentSize = (newCenters.size() * 90) / 100;
        logger.fatal("Percent value: " + percentSize);
        if (iConvergedCenters >= percentSize || avgValue < threshold)
            context.getCounter(CONVERGE_COUNTER.CONVERGED).increment(1);
        centerWriter.close();
        logger.fatal("Converged centers: " + iConvergedCenters);
    }

}
