package cs455.singletasks.loudest;

import cs455.hadoop.utils.TempFileReadMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class LoudestJob {

    // TODO - Remove HotTitleMapper if TempFileReadMapper works....

    /**
     * Which artist’s songs are the loudest on average?
     *
     * Main class to execute job.
     */
        public static void main(String[] args) {

            try {
                System.out.println("Running First Job..");
                Configuration conf = new Configuration();
                Job job1 = Job.getInstance(conf, "Initial Loudest-Artist");
                job1.setJarByClass(LoudestJob.class);

                // MultipleInputs for Mapper. Need to join two data on song_id.
                // arg0 is /analysis
                // arg1 is /metadata
                MultipleInputs.addInputPath(job1, new Path(args[0]),TextInputFormat.class, LoudestMapper.class);
                MultipleInputs.addInputPath(job1, new Path(args[1]),TextInputFormat.class, ArtistMapper.class);

                // Combiner. We use the reducer as the combiner in this case.
//                job.setCombinerClass(LoudestReducer.class);

                // Reducer
                job1.setNumReduceTasks(5);
                job1.setReducerClass(LoudestReducer.class);

                // Outputs types <key, value> from the Mapper.
                job1.setMapOutputKeyClass(Text.class);
                job1.setMapOutputValueClass(Text.class);

                // Outputs <key,value> from Reducer.
                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(IntWritable.class);

                // path to output in HDFS
                Path intermediatePath = new Path(args[2]+"-intermediate");
                FileOutputFormat.setOutputPath(job1, intermediatePath);
                job1.waitForCompletion(true);

                /** Second Job */
                System.out.println("Running second job..");
                Job job2 = Job.getInstance(conf, "Final Loudest-Artist");
                job2.setJarByClass(LoudestJob.class);
                job2.setMapperClass(TempFileReadMapper.class);
                job2.setReducerClass(LoudestArtistReducer.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(DoubleWritable.class);
                FileInputFormat.addInputPath(job2,intermediatePath);
                FileOutputFormat.setOutputPath(job2, new Path(args[2]));

               // Block until the job is completed.
                System.exit(job2.waitForCompletion(true) ? 0 : 1);

            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                System.err.println(e.getMessage());
            }

        }
}

