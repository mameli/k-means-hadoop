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
        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Path centers = new Path("centers/c.seq");

        conf.set("centersFilePath", centers.toString());
        Job job = Job.getInstance(conf, "K means");
        System.out.println("Input dir: " + args[0]);
        System.out.println("Output dir: " + args[1]);

        job.setJarByClass(KMeans.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(Reduce.class);

        /*
            Cleanup
         */
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            System.out.println("Delete old output folder: " + args[1]);
            fs.delete(output, true);
        }
        if (fs.exists(centers)) {
            System.out.println("Delete old centers folder: centers");
            fs.delete(centers, true);
        }
        System.out.println("Generate random centers");
        int k = Integer.parseInt(args[2]);
        createCenters(k, conf, centers);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.setMapOutputKeyClass(Center.class);
        job.setMapOutputValueClass(Point.class);
        job.setOutputKeyClass(Center.class);
        job.setOutputValueClass(Point.class);

        job.waitForCompletion(true);
    }

    private static void createCenters(int k, Configuration conf, Path centers) throws IOException {
        SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(centers),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Center.class));
//        Random r = new Random();
        Double randomValueX = 1.0;//Math.floor(10.0 * r.nextDouble() * 100) / 100;
        Double randomValueY = 3.0;//Math.floor(10.0 * r.nextDouble() * 100) / 100;
        for (int i = 0; i < k; i++) {
            centerWriter.append(new IntWritable(i),
                    new Center(new DoubleWritable(randomValueX), new DoubleWritable(randomValueY))
            );
            randomValueX = 7.0;//Math.floor(5.0 * r.nextDouble() * 100) / 100;
            randomValueY = 5.0;//Math.floor(5.0 * r.nextDouble() * 100) / 100;
        }
        centerWriter.close();
    }
}
