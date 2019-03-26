package cs455.hadoop.songsCount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */

public class SongsCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int tmpCount = 0;
    private String topArtist = "";
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
        for(IntWritable val : values){
            count += val.get();
        }
        if(count > tmpCount) {
            tmpCount = count;
            topArtist = key.toString();
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        // write the word with the highest frequency
        context.write(new Text(topArtist.split("%")[1]), new IntWritable(tmpCount));
    }
}
