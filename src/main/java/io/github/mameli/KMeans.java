package io.github.mameli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

/**
 * Created by mameli on 17/02/2017.
 * Main class
 */
public class KMeans {

    public static class Map extends Mapper<Object, Text, Point, Center> {
        private Logger logger = Logger.getLogger(Map.class);
        private List<Center> centers = new ArrayList<Center>();

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            logger.error(conf.get("centersFilePath"));
            Path centersPath = new Path(conf.get("centersFilePath"));
            logger.error("REEEEEEEEEEEEEEE");
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centersPath));
            IntWritable key = new IntWritable();
            Center value = new Center();
            while (reader.next(key, value)) {
                Center c = new Center(value.getX(), value.getY());
                c.setIndex(key.get());
                logger.info(c.getIndex());
                centers.add(c);
            }
            reader.close();
            logger.error("Centri: " + centers.toString());
            logger.error("Setup end");
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            List<Double> spaceValues = new ArrayList<Double>();
            StringTokenizer tokenizer = new StringTokenizer(line, ",");
            String temp;
            while (tokenizer.hasMoreTokens()) {
                temp = tokenizer.nextToken();
                spaceValues.add(Double.parseDouble(temp));
            }
            Center c = new Center(new DoubleWritable(0.0), new DoubleWritable(0.0));
//            logger.error(c.toString());
            Point p = new Point(new DoubleWritable(spaceValues.get(0)), new DoubleWritable(spaceValues.get(1)));
            logger.error(p.toString());
            context.write(p, c);
        }
    }

    public static class Reduce extends Reducer<Point, Point, DoubleWritable, DoubleWritable> {
        public void reduce(Point key, Iterable<Point> values, Context context)
                throws IOException, InterruptedException {
//            Logger logger = Logger.getLogger(Reduce.class);
//            logger.error("bla");
//            Double sum = 0.0;
//            for (Point val : values)
//                sum += val.getX();
//            context.write(new DoubleWritable(sum), new DoubleWritable(sum));
        }
    }

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
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(Center.class);
        job.setOutputKeyClass(Point.class);
        job.setOutputValueClass(Center.class);

        job.waitForCompletion(true);
    }

    private static void createCenters(int k, Configuration conf, Path centers) throws IOException {
        SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(centers),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Center.class));
        Random r = new Random();
        double randomValueX = 0.0 + (100.0 - 0.0) * r.nextDouble();
        double randomValueY = 0.0 + (100.0 - 0.0) * r.nextDouble();
        for (int i = 0; i < k; i++) {
            centerWriter.append(new IntWritable(i),
                    new Center(new DoubleWritable(randomValueX), new DoubleWritable(randomValueY))
            );
            randomValueX = 0.0 + (100.0 - 0.0) * r.nextDouble();
            randomValueY = 0.0 + (100.0 - 0.0) * r.nextDouble();
        }
        centerWriter.close();
    }
}
