package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.*;


public class RepJoin extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(RepJoin.class);

    public static class RSMapper extends Mapper<Object, Text, Text, Text> {
        private final static Text one = new Text("1");
        HashMap<Integer, List<Integer>> hashMap = new HashMap<>();
        private final int MAX = 400;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            URI[] localPaths = context.getCacheFiles();
            Path path = new Path(localPaths[0].toString());

            try (BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path.toString()))))) {
                String line;
                while ((line = bf.readLine()) != null) {
                    final String[] tokens = line.split(",");
                    Integer key = Integer.parseInt(tokens[0]);
                    Integer value = Integer.parseInt(tokens[1]);
                    if (key <= MAX && value <= MAX) {
                        if (!hashMap.containsKey(key)) {
                            hashMap.put(key, new ArrayList<>());
                            hashMap.get(key).add(value);
                        } else {
                            hashMap.get(key).add(value);
                        }
                    }
                }
            }
        }

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

            final String[] tokens = value.toString().split(",");
            Integer hashKey = Integer.parseInt(tokens[0]);
            Integer hashValue = Integer.parseInt(tokens[1]);
            if (hashKey <= MAX && hashValue <= MAX) {
                List<Integer> list = hashMap.get(hashValue);
                for (Integer l : list) {
                    if (hashMap.get(l).contains(hashKey)) {
                        context.getCounter(RepJoin.TriangleCount.NUMBER_OF_TRIANGLES).increment(1);
                    }
                }
            }
            Counter counter = context.getCounter(TriangleCount.NUMBER_OF_TRIANGLES);
            Long triangleCount = counter.getValue() / 3;
            context.write(new Text("Triangle Count "), new Text(triangleCount.toString()));

        }


        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

            context.write(one, new Text("c"));
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Twitter Joins");
        job.setJarByClass(RepJoin.class);
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
        //job.setCombinerClass(RSReducer.class);
        //job.setReducerClass(RSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new Path(args[2]).toUri());
        //return job.waitForCompletion(true) ? 0 : 1;
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
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new RepJoin(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
