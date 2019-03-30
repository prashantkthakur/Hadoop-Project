package cs455.hadoop.loudest;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper.
 * Generate intermediate data with <artist_info, loudness>
 *     This is intermediate data on which other mapper and reducer
 *     would work to find artist with highest average loudness.
 */

public class LoudestReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String idName = "";
//        String songId = "";
        double loudness = 0.0;
//        long count = 0;
        // calculate the total count
        for (Text val : values) {
            // Get different inputs from mapper
            String[] splits = val.toString().split("#-#");
            if (splits[0].equals("loudest")){
//                count++;
                loudness = Double.parseDouble(splits[1]);
            }
            else if (splits[0].equals("name")){
                idName = splits[1];
            }
        }
        context.write(new Text(idName), new DoubleWritable(loudness));

    }
}
