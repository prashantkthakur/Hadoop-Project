package cs455.hadoop.Job10;

import cs455.singletasks.fading.FadeTimeMapper;
import cs455.singletasks.loudest.ArtistMapper;
import cs455.singletasks.songsCount.SongsCountCombiner;
import cs455.singletasks.songsCount.SongsCountMapper;
import cs455.singletasks.songsCount.SongsCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Main class to execute job.
 */
public class FinalJob {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "Job10");
            // Current class.
            job.setJarByClass(FinalJob.class);
            // MultipleInputs for Mapper. Need to join two data on song_id.
            // arg0 is /analysis
            // arg1 is /metadata
            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AnalysisReader.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MetaDataReader.class);

            // Reducer
            job.setNumReduceTasks(1);
            job.setReducerClass(FinalReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // path to input in HDFS
//            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }
}
