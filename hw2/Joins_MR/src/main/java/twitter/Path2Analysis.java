package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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


public class Path2Analysis extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(Path2Analysis.class);

    public static class RSMapper extends Mapper<Object, Text, Text, Text> {
        private HashMap<Integer, Integer> followingCount;
        private HashMap<Integer, Integer> followedCount;

        @Override
        public void setup(Context c) throws IOException, InterruptedException {
           followingCount = new HashMap();
           followedCount = new HashMap();
        }

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] tokens = value.toString().split(",");
            int follower = Integer.parseInt(tokens[0]);
            int followee = Integer.parseInt(tokens[1]);
            if (!followedCount.containsKey(follower)) {
                followedCount.put(follower, 1);
            } else {
                followedCount.put(follower, followedCount.get(follower) + 1);
            }
            if (!followingCount.containsKey(followee)) {
                followingCount.put(followee, 1);
            } else {
                followingCount.put(followee, followingCount.get(followee) + 1);
            }
        }


        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<Map.Entry<Integer, Integer>> iterator = followedCount.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Integer> next = iterator.next();
                if (followingCount.containsKey(next.getKey())) {
                    String val = next.getValue().toString() + "," + followingCount.get(next.getKey()).toString();
                    context.write(new Text(next.getKey().toString()), new Text(val));
                    followingCount.remove(next.getKey());
                } else {
                    String val = next.getValue().toString() + ",0";
                    context.write(new Text(next.getKey().toString()), new Text(val));
                }
            }

            Iterator<Map.Entry<Integer, Integer>> followingCountIterator = followingCount.entrySet().iterator();
            while (followingCountIterator.hasNext()) {
                Map.Entry<Integer, Integer> next = followingCountIterator.next();
                String val = "0," + next.getValue().toString();
                context.write(new Text(next.getKey().toString()), new Text(val));
            }
        }
    }

    public static class RSReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            int noOfFollowers = 0;
            int noOfFollowees = 0;
            for (Text val : values) {
                String[] substrs = val.toString().split(",");
                noOfFollowees += Integer.parseInt(substrs[0]);
                noOfFollowers += Integer.parseInt(substrs[1]);
            }
            Integer cardinality;
            if (noOfFollowees == 0) {
                cardinality = noOfFollowers;
            } else if (noOfFollowers == 0){
                cardinality = noOfFollowees;
            } else {
                cardinality = noOfFollowees * noOfFollowers;
            }
            context.write(key, new Text(cardinality.toString()));
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Twitter Joins");
        job.setJarByClass(Path2Analysis.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        // Delete output directory, only to ease local development; will not work on AWS.
        // ===========
        final FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path(args[1]))) {
            fileSystem.delete(new Path(args[1]), true);
        }
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
            ToolRunner.run(new Path2Analysis(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
