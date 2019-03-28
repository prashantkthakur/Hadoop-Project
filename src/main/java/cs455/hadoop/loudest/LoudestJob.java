package cs455.hadoop.loudest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class LoudestJob {


    /**
     * Which artist’s songs are the loudest on average?
     *
     * Main class to execute job.
     */
        public static void main(String[] args) {
            if (args.length < 3 || args.length > 4 ){
                System.err.println("Number of arguments should be 3.\n" +
                        "hdfs path for Analysis\t hdfs path for metadata\t hdfs path for output");
            try {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "Loudest Artist");
                job.setJarByClass(LoudestJob.class);

                // MultipleInputs for Mapper. Need to join two data on song_id.
                MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, LoudestMapper.class);
                MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, ArtistMapper.class);

                // Combiner. We use the reducer as the combiner in this case.
//                job.setCombinerClass(LoudestReducer.class);

                // Reducer
                job.setNumReduceTasks(1);
                job.setReducerClass(LoudestReducer.class);

                // Outputs types <key, value> from the Mapper.
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                // Outputs <key,value> from Reducer.
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                // path to output in HDFS
                Path outputPath = new Path(args[2]);
                FileOutputFormat.setOutputPath(job, outputPath);

                // Block until the job is completed.
                System.exit(job.waitForCompletion(true) ? 0 : 1);

            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                System.err.println(e.getMessage());
            }

        }
    }

}