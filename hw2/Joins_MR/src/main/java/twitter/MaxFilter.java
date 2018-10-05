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
import java.util.StringTokenizer;


public class MaxFilter extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(MaxFilter.class);

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private final Text follower = new Text();
        private final Text followee = new Text();
        private final int MAX = 1000;

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] tokens = value.toString().split(",");
            if(Integer.parseInt(tokens[0]) <= MAX && Integer.parseInt(tokens[1]) <= MAX) {
                follower.set(tokens[0]);
                followee.set(tokens[1]);
                context.write(follower, followee);
            }
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Twitter Follower");
        job.setJarByClass(MaxFilter.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        // Delete output directory, only to ease local development; will not work on AWS.
        // ===========
        final FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path(args[1]))) {
        	fileSystem.delete(new Path(args[1]), true);
        }
        // ================
        job.setMapperClass(TokenizerMapper.class);
        // Set reducer and combiner as same as we are just aggregating the values
        //job.setCombinerClass(IntSumReducer.class);
        //job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new MaxFilter(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
