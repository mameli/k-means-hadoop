package io.github.mameli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by mameli on 17/02/2017.
 * Main class
 */
public class KMeans {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Path centers = new Path(input.getParent().toString() + "centers/c.seq");

        conf.set("centersFilePath", centers.toString());
        conf.setDouble("threshold", Double.parseDouble(args[4]));

        int k = Integer.parseInt(args[2]);
        conf.setInt("k", k);
        int iCoordinates = Integer.parseInt(args[3]);
        conf.setInt("iCoordinates", iCoordinates);

        Job job;

        FileSystem fs = FileSystem.get(output.toUri(),conf);
        if (fs.exists(output)) {
            System.out.println("Delete old output folder: " + output.toString());
            fs.delete(output, true);
        }

        createCenters(k, conf, centers);

        long isConverged = 0;
        int iterations = 0;
        while (isConverged != 1) {
            job = Job.getInstance(conf, "K means iter");
            job.setJarByClass(KMeans.class);
            job.setMapperClass(Map.class);
            job.setCombinerClass(Combine.class);
            job.setReducerClass(Reduce.class);

            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            job.setMapOutputKeyClass(Center.class);
            job.setMapOutputValueClass(Point.class);

            job.waitForCompletion(true);

            isConverged = job.getCounters().findCounter(Reduce.CONVERGE_COUNTER.CONVERGED).getValue();

            fs.delete(output, true);
            iterations++;
        }

        job = Job.getInstance(conf, "K means map");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(Map.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.setMapOutputKeyClass(Center.class);
        job.setMapOutputValueClass(Point.class);

        job.waitForCompletion(true);

        fs.delete(centers.getParent(), true);
        System.out.println("Number of iterations\t" + iterations);
    }

    private static void createCenters(int k, Configuration conf, Path centers) throws IOException {
        SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(centers),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Center.class));
        Random r = new Random();
        List<DoubleWritable> listParameters = new ArrayList<DoubleWritable>();
        Center tempC;
        Double temp;
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < conf.getInt("iCoordinates", 2); j++) {
                temp = Math.floor(100.0 * r.nextDouble() * 100) / 100;
                listParameters.add(new DoubleWritable(temp));
            }
            tempC = new Center(listParameters, new IntWritable(i), new IntWritable(0));
            centerWriter.append(new IntWritable(i), tempC);
            listParameters = new ArrayList<DoubleWritable>();
        }
        centerWriter.close();
    }
}
