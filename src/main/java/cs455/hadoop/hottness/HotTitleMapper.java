package cs455.hadoop.hottness;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HotTitleMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
        String[] val = value.toString().split("\t");
        double hotness = Double.parseDouble(val[1]);
        // Send to reducer <song_id%%title, hotness>
        context.write(new Text(val[0].trim()), new DoubleWritable(hotness));
    }
}
