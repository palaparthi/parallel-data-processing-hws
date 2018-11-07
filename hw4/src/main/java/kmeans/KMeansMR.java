package kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
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

public class KMeansMR extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(KMeansMR.class);

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        List<Long> centroids = new ArrayList<>();

        @Override
        public void setup(Context context) throws IOException {
            URI[] cache = context.getCacheFiles();
            if (cache != null && cache.length > 0) {
                for (int j = 0; j < cache.length; j++) {
                    final Configuration conf = context.getConfiguration();
                    // read cache file
                    FileSystem fileSystem = FileSystem.get(cache[j], conf);
                    Path path = new Path(cache[j].toString());
                    BufferedReader bf = null;
                    try {
                        bf = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
                        String line;
                        while ((line = bf.readLine()) != null) {
                            //add centroid list
                            Long l = Long.parseLong(line.split("\\s+")[0]);
                            centroids.add(l);
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    } finally {
                        if (bf != null) {
                            bf.close();
                        }
                    }
                }
            }
        }

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            Long closestCenter = centroids.get(0);
            final String[] tokens = value.toString().split("\\s+");
            Long val = Long.parseLong(tokens[1]);
            Long minDist = Math.abs(closestCenter - val);

            for (int i = 0; i < centroids.size(); i++) {
                // find closest center
                if (Math.abs(centroids.get(i) - val) < minDist) {
                    closestCenter = centroids.get(i);
                    minDist = Math.abs(closestCenter - val);
                }
            }
            context.write(new Text(closestCenter.toString()), new Text(val.toString()));
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            Text newCentroid = new Text();
            Long sum = 0l;
            Long SSE = 0l;
            Long n = 0l;
            Long centroid = Long.parseLong(key.toString());

            for (Text value : values) {
                // update sum and SSE
                sum += Long.parseLong(value.toString());
                SSE += ((centroid - Long.parseLong(value.toString())) * (centroid - Long.parseLong(value.toString())));
                n += 1;
            }


            context.getCounter(KMeans.SSE).increment(SSE);
            // set new centroid. depending on values
            newCentroid.set(new Text(String.valueOf(sum/n)));

            context.write(newCentroid, new Text(String.valueOf(n)));
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        int retcode = 0;
        int i = 0;
        while (i < 10) {
            final Job job = Job.getInstance(conf, "KMeans");
            job.setJarByClass(KMeansMR.class);
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
            job.setMapperClass(KMeansMapper.class);
            // Set reducer and combiner as same as we are just aggregating the values
            //job.setCombinerClass(KMeansReducer.class);
            job.setReducerClass(KMeansReducer.class);
            //job.addCacheFile(new Path(args[2]).toUri());
            RemoteIterator<LocatedFileStatus> remoteIterator;

            // create output directories for every run
            if (i == 0) {
                centroidPath = new Path(args[1]);
                fileSystem = FileSystem.get(centroidPath.toUri(), conf);
                remoteIterator = fileSystem.listFiles(new Path(args[1]), false);
            } else {
                centroidPath = new Path(args[1] + String.valueOf(i - 1));
                fileSystem = FileSystem.get(centroidPath.toUri(), conf);
                remoteIterator = fileSystem.listFiles(new Path(args[1] + String.valueOf(i - 1)), false);

            }

            while (remoteIterator.hasNext()) {
                job.addCacheFile(remoteIterator.next().getPath().toUri());
            }

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + String.valueOf(i)));
            retcode = job.waitForCompletion(true) ? 0 : 1;

            Counters cn = job.getCounters();
            Counter counter = cn.findCounter(KMeans.SSE);
            logger.info("SSE value " + counter.getValue());
            i += 1;
        }

        return retcode;
    }


    enum KMeans {
        SSE,
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new KMeansMR(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}


