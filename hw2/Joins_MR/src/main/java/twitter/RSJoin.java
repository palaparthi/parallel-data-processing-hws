package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


public class RSJoin extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(RSJoin.class);

    public static class RSMapper extends Mapper<Object, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();
        private final Text outKey2 = new Text();
        private final Text outValue2 = new Text();
        private final int MAX = 5000;

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] tokens = value.toString().split(",");
            if (Integer.parseInt(tokens[0]) <= MAX && Integer.parseInt(tokens[1]) <= MAX) {
                outKey.set(tokens[1]);
                outValue.set("F" + tokens[0]);
                outKey2.set(tokens[0]);
                outValue2.set("T" + tokens[1]);
                context.write(outKey, outValue);
                context.write(outKey2, outValue2);
            }
        }
    }

    public static class RSReducer extends Reducer<Text, Text, Text, Text> {
        private final IntWritable result = new IntWritable();
        private List<Text> fromList = new ArrayList();
        private List<Text> toList = new ArrayList();


        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            fromList.clear();
            toList.clear();

            for (Text t : values) {
                if (t.charAt(0) == 'F') {
                    fromList.add(new Text(t.toString().substring(1)));
                } else if (t.charAt(0) == 'T') {
                    toList.add(new Text(t.toString().substring(1)));
                }
            }

            if (!fromList.isEmpty() && !toList.isEmpty()) {
                for (Text F : fromList) {
                    for (Text T : toList) {
                        //if (Integer.parseInt(F.toString()) != Integer.parseInt(T.toString())) {
                            context.write(F, T);
                        //}
                    }
                }
            }
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Twitter Joins");
        job.setJarByClass(RSJoin.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        // Delete output directory, only to ease local development; will not work on AWS.
        // ===========
//        final FileSystem fileSystem = FileSystem.get(conf);
//        if (fileSystem.exists(new Path(args[1]))) {
//            fileSystem.delete(new Path(args[1]), true);
//        }
        // ================
        job.setMapperClass(RSMapper.class);
        // Set reducer and combiner as same as we are just aggregating the values
        //job.setCombinerClass(RSReducer.class);
        job.setReducerClass(RSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new RSJoin(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
