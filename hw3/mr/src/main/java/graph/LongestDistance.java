package graph;

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
import java.util.*;
import java.util.stream.Collectors;

public class LongestDistance extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(LongestDistance.class);

    public static class LongestDistanceMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

            final String[] tokens = value.toString().split("\\s+");
            // key = 1 because we want to send everything to the same reducer to calculate the longest shortest path
            context.write(new Text("1"), new Text(tokens[2]));

        }
    }

    public static class LongestDistanceReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text node, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            Text LongestDistance = new Text();
            int longest = 0;

            for(Text value : values) {
                if (Integer.parseInt(value.toString()) != Integer.MAX_VALUE){
                    if (longest < Integer.parseInt(value.toString())){
                        longest = Integer.parseInt(value.toString());
                    }
                }
            }


            LongestDistance.set(new Text(String.valueOf(longest)));

            context.write(node, LongestDistance);
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Graph Diameter");
        job.setJarByClass(LongestDistance.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        // Delete output directory, only to ease local development; will not work on AWS.
        // ===========
//        final FileSystem fileSystem = FileSystem.get(conf);
//        if (fileSystem.exists(new Path(args[1]))) {
//            fileSystem.delete(new Path(args[1]), true);
//        }
        // ================
        job.setMapperClass(LongestDistanceMapper.class);
        // Set reducer and combiner as same as we are just aggregating the values
        job.setCombinerClass(LongestDistanceReducer.class);
        job.setReducerClass(LongestDistanceReducer.class);
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
            ToolRunner.run(new LongestDistance(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
