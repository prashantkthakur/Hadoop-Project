package cs455.hadoop.combined;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CombinedJob {

    public static void main(String[] args) {

        try {
//            double maxHottness = 1.0;
//            System.out.println("Running First Job..");
//            if (args.length == 4) {
//               maxHottness = Double.parseDouble(args[2]);
//            }
            Configuration conf = new Configuration();
            Job job1 = Job.getInstance(conf);
            job1.setJarByClass(CombinedJob.class);

            // MultipleInputs for Mapper. Need to join two data on song_id.
            // arg0 is /analysis
            // arg1 is /metadata
//            FileInputFormat.addInputPath(job1, new Path(args[0]));
//            job1.setMapperClass(AvgAnalysisMapper.class);
            MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, CombinedAnalysisReader.class);
            MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, CombinedMetaDataReader.class);


            // Reducer
            job1.setNumReduceTasks(1);
            job1.setReducerClass(CombinedReducer.class);

            // Outputs types <key, value> from the Mapper.
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);

            // Outputs <key,value> from Reducer.
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            // Remove part-r-0000X file while using MultipleOutputs.
            LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);

            // path to output in HDFS
            MultipleOutputs.addNamedOutput(job1, "data", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job1, "avg", TextOutputFormat.class, Text.class, Text.class);
            FileOutputFormat.setOutputPath(job1, new Path(args[2]));



            // Block until the job is completed.
            System.exit(job1.waitForCompletion(true) ? 0 : 1);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
