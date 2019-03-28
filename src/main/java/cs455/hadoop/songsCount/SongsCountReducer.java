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
    private IntWritable tmpCount = new IntWritable();
    private Text topArtist = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
            // calculate the total count
            for (IntWritable val : values) {
                count += val.get();
            }
            if (count > tmpCount.get()) {
                tmpCount.set(count);
                if (key.toString().contains("%")) {
                    topArtist.set(key.toString().split("%%")[1]);
                }
//                System.out.println(tmpCount+"TOP ARTIST: " + topArtist + " :: " + key);
//                context.write(topArtist, new IntWritable(tmpCount));

            }


    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(topArtist, tmpCount);

    }
}
