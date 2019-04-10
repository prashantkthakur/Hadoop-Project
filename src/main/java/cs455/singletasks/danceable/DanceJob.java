package cs455.singletasks.danceable;

import cs455.singletasks.hottness.TitleMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DanceJob {


    /**
     * What are the 10 most energetic and danceable songs? List them in descending order.
     * <p>
     * Main class to execute job.
     */
    public static void main(String[] args) {

        try {
            System.out.println("Running Job..");
            Configuration conf = new Configuration();
            Job job1 = Job.getInstance(conf, "Initial Dance-Songs");
            job1.setJarByClass(DanceJob.class);

            // MultipleInputs for Mapper. Need to join two data on song_id.
            // arg0 is /analysis
            // arg1 is /metadata
            MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, DanceMapper.class);
            MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, TitleMapper.class);

            // Reducer
            job1.setNumReduceTasks(1);
            job1.setReducerClass(DanceReducer.class);

            // Outputs types <key, value> from the Mapper.
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);

            // Outputs <key,value> from Reducer.
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(DoubleWritable.class);

            Path intermediatePath = new Path(args[2]);
            FileOutputFormat.setOutputPath(job1, intermediatePath);
            job1.waitForCompletion(true);
            // Block until the job is completed.
            System.exit(job1.waitForCompletion(true) ? 0 : 1);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
