package cs455.hadoop.fading;


import cs455.hadoop.hottness.HotTitleMapper;
import cs455.hadoop.hottness.HotTitleReducer;
import cs455.hadoop.loudest.ArtistMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class FadingJob {


    /**
     * What is the song with the highest hotttnesss (popularity) score
     * <p>
     * Main class to execute job.
     */
    public static void main(String[] args) {

        try {
            System.out.println("Running First Job..");
            Configuration conf = new Configuration();
            Job job1 = Job.getInstance(conf, "Initial Fading");
            job1.setJarByClass(FadingJob.class);

            // MultipleInputs for Mapper. Need to join two data on song_id.
            // arg0 is /analysis
            // arg1 is /metadata
            MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, FadeTimeMapper.class);
            MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, ArtistMapper.class);

            // Combiner. We use the reducer as the combiner in this case.
//                job.setCombinerClass(LoudestReducer.class);

            // Reducer
            job1.setNumReduceTasks(10);
            job1.setReducerClass(FadingReducer.class);

            // Outputs types <key, value> from the Mapper.
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);

            // Outputs <key,value> from Reducer.
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(DoubleWritable.class);

            // path to output in HDFS
            Path intermediatePath = new Path(args[2] + "-tmp");
            FileOutputFormat.setOutputPath(job1, intermediatePath);
            job1.waitForCompletion(true);

            /** Second Job */

            System.out.println("Running second job..");
            Job job2 = Job.getInstance(conf, "Final Fading");
            job2.setJarByClass(FadingJob.class);
            job2.setMapperClass(HotTitleMapper.class);
            job2.setReducerClass(HotTitleReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);
            job2.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job2, intermediatePath);
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            // Block until the job is completed.
            System.exit(job2.waitForCompletion(true) ? 0 : 1);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
