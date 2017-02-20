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
import java.util.Random;

/**
 * Created by mameli on 17/02/2017.
 * Main class
 */
public class KMeans {

    public static void main(String[] args) throws Exception {
        /*
            Setup
        */
        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Path centers = new Path("centers/c.seq");

        conf.set("centersFilePath", centers.toString());
        conf.setDouble("threshold", 0.01);

        int k = Integer.parseInt(args[2]);
        conf.setInt("k", k);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            System.out.println("Delete old output folder: " + args[1]);
            fs.delete(output, true);
        }
        if (fs.exists(centers)) {
            System.out.println("Delete old centers folder: centers");
            fs.delete(centers, true);
        }

        createCenters(k, conf, centers);

        long isConverged = 0;

        while (isConverged != 1) {
            Job job = Job.getInstance(conf, "K means");
            job.setJarByClass(KMeans.class);
            job.setMapperClass(Map.class);
            job.setCombinerClass(Combine.class);
            job.setNumReduceTasks(1);
            job.setReducerClass(Reduce.class);

            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            job.setMapOutputKeyClass(Center.class);
            job.setMapOutputValueClass(Point.class);

            job.waitForCompletion(true);

            isConverged = job.getCounters().findCounter(Reduce.CONVERGE_COUNTER.CONVERGED).getValue();
            if (isConverged != 1)
                fs.delete(output, true);
        }

        System.out.println("Output results: Centers and linked points");
        fs.delete(output, true);
        Job job = Job.getInstance(conf, "K means");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(Map.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.setMapOutputKeyClass(Center.class);
        job.setMapOutputValueClass(Point.class);

        job.waitForCompletion(true);
    }

    private static void createCenters(int k, Configuration conf, Path centers) throws IOException {
        SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(centers),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Center.class));
        Random r = new Random();
        Double randomValueX = Math.floor(100.0 * r.nextDouble() * 100) / 100;
        Double randomValueY = Math.floor(100.0 * r.nextDouble() * 100) / 100;
        for (int i = 0; i < k; i++) {
            centerWriter.append(new IntWritable(i),
                    new Center(new DoubleWritable(randomValueX), new DoubleWritable(randomValueY))
            );
            randomValueX = Math.floor(100.0 * r.nextDouble() * 100) / 100;
            randomValueY = Math.floor(100.0 * r.nextDouble() * 100) / 100;
        }
        centerWriter.close();
    }
}
