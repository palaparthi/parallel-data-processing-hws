package twitter;

import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;



public class RSJoinTriangle extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(RSJoinTriangle.class);

    public static class Path2 extends Mapper<Object, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();
        private final int MAX = 1000;

        @Override
        public void setup(Context c) throws IOException, InterruptedException {


        }

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] tokens = value.toString().split("\\s+");
            if (Integer.parseInt(tokens[0]) <= MAX && Integer.parseInt(tokens[1]) <= MAX) {
                outKey.set(tokens[0] + "," + tokens[1]);
                outValue.set("P");
                context.write(outKey, outValue);
            }
        }
    }

    public static class Edges extends Mapper<Object, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();
        private final int MAX = 1000;

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] tokens = value.toString().split(",");
            if (Integer.parseInt(tokens[0]) <= MAX && Integer.parseInt(tokens[1]) <= MAX) {
                outKey.set(tokens[1] + "," + tokens[0]);
                outValue.set("E");
                context.write(outKey, outValue);
            }
        }
    }

    public static class TriangleReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            boolean isP = false;
            boolean isE = false;
            for (Text t : values) {
                if (!isE || !isP) {
                    if (t.toString().equals("P")) {
                        isP = true;
                    } else if (t.toString().equals("E")) {
                        isE = true;
                    }
                }
            }

            if (!key.toString().isEmpty() && (isE && isP)) {
                context.write(key, new Text("1"));
                context.getCounter(TriangleCount.NUMBER_OF_TRIANGLES).increment(1);
            }

        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Twitter Joins");
        job.setJarByClass(RSJoinTriangle.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        // Delete output directory, only to ease local development; will not work on AWS.
        // ===========
//        final FileSystem fileSystem = FileSystem.get(conf);
//        if (fileSystem.exists(new Path(args[1]))) {
//            fileSystem.delete(new Path(args[1]), true);
//        }
        // ================
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Path2.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Edges.class);

        //job.setMapperClass(Path2.class);
        // Set reducer and combiner as same as we are just aggregating the values
        //job.setCombinerClass(TriangleReducer.class);
        job.setReducerClass(TriangleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
        Counters cn = job.getCounters();
        Counter counter = cn.findCounter(TriangleCount.NUMBER_OF_TRIANGLES);
        Long triangleCount = counter.getValue() / 3;
        logger.info("TRIANGLE COUNT " + triangleCount);
        return 0;
    }

    enum TriangleCount {
        NUMBER_OF_TRIANGLES
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Three arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new RSJoinTriangle(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
