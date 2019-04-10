package cs455.singletasks.loudest;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class LoudestArtistMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split("\t");
        double loudness = Double.parseDouble(val[1]);
        // Send to reducer <artist_id%%artist_name, loudness>
        context.write(new Text(val[0].trim()), new DoubleWritable(loudness));
    }
}
