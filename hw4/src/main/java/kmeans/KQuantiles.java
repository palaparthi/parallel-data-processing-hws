package kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class KQuantiles extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(KQuantiles.class);

    public static class KQuantilesMapper extends Mapper<Object, Text, Text, Text> {
        List<Long> centroids = new ArrayList<>();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] tokens = value.toString().split("\\s+");
            context.write(new Text("1"), new Text(tokens[1]));
        }
    }

    public static class KQuantilesReducer extends Reducer<Text, Text, Text, Text> {
        String K = "";
        @Override
        public void setup(Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            K = conf.get("K");
        }

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            int noOfQuantiles = Integer.parseInt(K);
            int min = 0;
            int max = 0;
            for (Text value : values) {
                int val = Integer.parseInt(value.toString());
                // update min and max values
                if (val < min){
                    min = val;
                } else if (val > max){
                    max = val;
                }
            }
            // create equal intervals
            int interval = (min + max)/noOfQuantiles;
            context.write(new Text(String.valueOf(min)), new Text(String.valueOf(min)));
            for(int i=0; i< noOfQuantiles; i++){
                min += interval;
                Text newKey = new Text(String.valueOf(min));
                context.write(newKey, newKey);
            }
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("K", args[2]);
        int retcode = 0;
        final Job job = Job.getInstance(conf, "KQuantiles");
        job.setJarByClass(KQuantiles.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        // Delete output directory, only to ease local development; will not work on AWS.
        // ===========
//        if (fileSystem.exists(new Path(args[1]))) {
//            fileSystem.delete(new Path(args[1]), true);
//        }
        // ================
        Path centroidPath;
        final FileSystem fileSystem;
        job.setMapperClass(KQuantilesMapper.class);
        // Set reducer and combiner as same as we are just aggregating the values
        job.setReducerClass(KQuantilesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
}


    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new KQuantiles(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
