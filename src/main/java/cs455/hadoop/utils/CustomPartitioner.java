package cs455.hadoop.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text, Text> {

    public int getPartition(Text key, Text value, int numReduceTasks) {
        if (numReduceTasks == 0)
            return 0;
        if (key.toString().contains("avg")) {
            return 0;
        }
        else {
            return 1;
        }
    }

}
