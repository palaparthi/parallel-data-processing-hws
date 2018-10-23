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

public class AdjacencyList extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(AdjacencyList.class);

    public static class AdjacencyMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

            final String[] tokens = value.toString().split(",");
            context.write(new Text(tokens[0]), new Text(tokens[1]));

        }
    }

    public static class AdjacencyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text node, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            Text adjacencyList = new Text();

            StringBuilder adjList = new StringBuilder();
            for(Text neighbor : values) {
                adjList.append(neighbor.toString());
                adjList.append(",");
            }

            String resultString = adjList.length() > 0 ? adjList.substring(0, adjList.length() -1): "";
            adjacencyList.set(resultString);

            context.write(node, adjacencyList);
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Graph Diameter");
        job.setJarByClass(AdjacencyList.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        // Delete output directory, only to ease local development; will not work on AWS.
        // ===========
//        final FileSystem fileSystem = FileSystem.get(conf);
//        if (fileSystem.exists(new Path(args[1]))) {
//            fileSystem.delete(new Path(args[1]), true);
//        }
        // ================
        job.setMapperClass(AdjacencyMapper.class);
        // Set reducer and combiner as same as we are just aggregating the values
        job.setCombinerClass(AdjacencyReducer.class);
        job.setReducerClass(AdjacencyReducer.class);
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
            ToolRunner.run(new AdjacencyList(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
