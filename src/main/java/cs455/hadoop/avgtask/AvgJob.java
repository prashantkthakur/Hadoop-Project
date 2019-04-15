package cs455.hadoop.avgtask;

import cs455.hadoop.multitask.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AvgJob {

    public static void main(String[] args) {

        try {
            double maxHottness = 1.0;
            System.out.println("Running First Job..");
            if (args.length == 4) {
               maxHottness = Double.parseDouble(args[2]);
            }
            Configuration conf = new Configuration();
            Job job1 = Job.getInstance(conf, "Initial Average-Calculations");
            job1.setJarByClass(AvgJob.class);

            // MultipleInputs for Mapper. Need to join two data on song_id.
            // arg0 is /analysis
            // arg1 is /metadata
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            job1.setMapperClass(AvgAnalysisMapper.class);
//            MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, AvgAnalysisMapper.class);
//            MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, AvgMetaDataMapper.class);

            // Combiner. We use the reducer as the combiner in this case.
//                job.setCombinerClass(LoudestReducer.class);

            // Reducer
            job1.setNumReduceTasks(1);
            job1.setReducerClass(AvgReducer.class);

            // Outputs types <key, value> from the Mapper.
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);

            // Outputs <key,value> from Reducer.
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            // path to output in HDFS
//            Path intermediatePath = new Path(args[2] + "-tmp");
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));
//            job1.waitForCompletion(true);

            /** Second Job */
//
//            System.out.println("Running second job..");
//            Job job2 = Job.getInstance(conf, "Final Average-Computation");
//            job2.setJarByClass(.class);
//            job2.setMapperClass(TempFileMapper.class);
//            job2.setReducerClass(SecondReducer.class);
//            job2.setOutputKeyClass(Text.class);
//            job2.setOutputValueClass(Text.class);
//            job2.setNumReduceTasks(1);
//            FileInputFormat.addInputPath(job2, intermediatePath);
//            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            // Block until the job is completed.
            System.exit(job1.waitForCompletion(true) ? 0 : 1);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
