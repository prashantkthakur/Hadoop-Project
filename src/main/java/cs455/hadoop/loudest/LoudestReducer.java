package cs455.hadoop.loudest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */

public class LoudestReducer extends Reducer<Text, Text, Text, IntWritable> {
    private IntWritable tmpCount = new IntWritable();
    private Text topArtist = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String name = "";
        String songId = "";
        double loudness = 0.0;
        long count = 0;
        // calculate the total count
        for (Text val : values) {
            // Get different inputs from mapper
            String[] splits = val.toString().split("\t");
            if (splits[0].equals("loudest")){
                count++;
                loudness = Double.parseDouble(splits[1]);
            }
            if (splits[0].equals("name")){
                name = splits[1];
            }



        }



    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(topArtist, tmpCount);

    }
}
