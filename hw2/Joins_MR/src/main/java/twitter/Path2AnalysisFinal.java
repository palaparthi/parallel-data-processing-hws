package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import java.util.*;


public class Path2AnalysisFinal extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(Path2AnalysisFinal.class);

    public static class RSMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static Text one = new Text("1");
        long sum;

        @Override
        public void setup(Context c) throws IOException, InterruptedException {
            sum = 0;
        }

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] tokens = value.toString().split("\\s+");
            sum += Long.parseLong(tokens[1]);
        }


        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // emit entire cardinality
            context.write(one, new LongWritable(sum));
        }
    }

        public static class RSReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
            @Override
            public void reduce(final Text key, final Iterable<LongWritable> values, final Context context) throws IOException, InterruptedException {
                Long cardinality = 0l;
                for (LongWritable val : values) {
                    cardinality += Long.parseLong(val.toString());
                }
                context.write(key, new LongWritable(cardinality));
            }
        }

        @Override
        public int run(final String[] args) throws Exception {
            //setup(context);
            final Configuration conf = getConf();
            final Job job = Job.getInstance(conf, "Twitter Joins");
            job.setJarByClass(Path2AnalysisFinal.class);
            final Configuration jobConf = job.getConfiguration();
            jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
            // Delete output directory, only to ease local development; will not work on AWS.
            // ===========
//            final FileSystem fileSystem = FileSystem.get(conf);
//            if (fileSystem.exists(new Path(args[1]))) {
//                fileSystem.delete(new Path(args[1]), true);
//            }
            // ================
            job.setMapperClass(RSMapper.class);
            // Set reducer and combiner as same as we are just aggregating the values
            job.setCombinerClass(RSReducer.class);
            job.setReducerClass(RSReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            //setup(context);
            return job.waitForCompletion(true) ? 0 : 1;
        }

        public static void main(final String[] args) {
            if (args.length != 2) {
                throw new Error("Two arguments required:\n<input-dir> <output-dir>");
            }

            try {
                ToolRunner.run(new Path2AnalysisFinal(), args);
            } catch (final Exception e) {
                logger.error("", e);
            } finally {
                //cleanup(context);
            }
        }

    }
