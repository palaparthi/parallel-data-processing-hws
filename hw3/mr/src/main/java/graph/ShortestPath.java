package graph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ShortestPath extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(ShortestPath.class);

    public static class ShortestPathMapper extends Mapper<Object, Text, Text, Text> {
        String source = "";

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            source = conf.get("SOURCE");
        }

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

            final String[] tokens = value.toString().split("\\s+");
            int infinity = Integer.MAX_VALUE;
            int distance = infinity;
            String from = tokens[0];
            String adjacencyList = tokens[1];

            if (tokens.length == 2) {
                if (from.equals(source)) {
                    distance = 0;
                } else {
                    distance = infinity;
                }
            } else {
                // reducer output
                distance = Integer.parseInt(tokens[2]);
            }

            // Pass the graph structure
            Text outKey = new Text(tokens[0]);
            Text outValue = new Text(String.join(" ", adjacencyList, String.valueOf(distance)));
            context.write(outKey, outValue);

            if (!adjacencyList.equals("null")) {
                String[] neighbors = adjacencyList.split(",");
                for (String n : neighbors) {
                    int newDistance = distance;
                    if (distance != infinity) {
                        newDistance += 1;
                        outKey.set(n);
                        outValue.set(String.valueOf(newDistance));
                        context.write(outKey, outValue);
                    }
                }
            }

        }
    }

    public static class ShortestPathReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text node, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

            String adjacencyList = null;
            int originalDistance = Integer.MAX_VALUE;
            int dMin = Integer.MAX_VALUE;

            for (Text value : values) {
                final String[] tokens = value.toString().split("\\s+");
                if (tokens.length == 2) {
                    adjacencyList = tokens[0];
                    originalDistance = Integer.parseInt(tokens[1]);
                } else {
                    int dist = Integer.parseInt(tokens[0]);
                    if (dist < dMin) {
                        dMin = dist;
                    }
                }
            }

            if (dMin < originalDistance) {
                originalDistance = dMin;
                context.getCounter(ShortestPath.GraphDiameter.NUMBER_OF_INFINITY_DISTANCES).increment(1);

            }
            Text outKey = new Text(node);
            Text outValue = new Text(String.join(" ", adjacencyList, String.valueOf(originalDistance)));
            context.write(outKey, outValue);
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("SOURCE", "1");
        int n = 1;
        int retcode = 0;
        Long infinityCount = 0l;
        while (infinityCount != -1 ) {
            final Job job = Job.getInstance(conf, "Graph Diameter");
            job.setJarByClass(ShortestPath.class);
            final Configuration jobConf = job.getConfiguration();
            jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
            String input, output;
            if (n == 1) {
                input = args[0];
            } else {
                input = args[1] + "_" + (n - 1);
            }
            output = args[1] + "_" + n;
            String oldOutput = args[1] + "_" + (n - 2);
            // Delete output directory, only to ease local development; will not work on AWS.
            // ===========
//            final FileSystem fileSystem = FileSystem.get(conf);
//            if (fileSystem.exists(new Path(output))) {
//                fileSystem.delete(new Path(output), true);
//            }
//            if (fileSystem.exists(new Path(oldOutput))) {
//                fileSystem.delete(new Path(oldOutput), true);
//            }
            // ================
            job.setMapperClass(ShortestPathMapper.class);
            // Set reducer and combiner as same as we are just aggregating the values
            job.setCombinerClass(ShortestPathReducer.class);
            job.setReducerClass(ShortestPathReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            retcode = job.waitForCompletion(true) ? 0 : 1;

            Counters cn = job.getCounters();
            Counter counter = cn.findCounter(GraphDiameter.NUMBER_OF_INFINITY_DISTANCES);
            //if (infinityCount == counter.getValue()){
            if (counter.getValue() == 0){
                infinityCount = -1l;
            } else{
                infinityCount = counter.getValue();
            }
            logger.info("Cnt Inf Count" + infinityCount);
            n++;
        }
        return retcode;
    }

    enum GraphDiameter {
        NUMBER_OF_INFINITY_DISTANCES,
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new ShortestPath(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}