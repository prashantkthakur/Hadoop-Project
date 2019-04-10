package cs455.singletasks.loudest;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LoudestArtistReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable tmpLoudness = new DoubleWritable();
    private Text topArtist = new Text();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        double avgloudness = 0.0;
        // calculate info required for computing average of loudness.
        for (DoubleWritable val : values) {
            count++;
            avgloudness += val.get();
        }
        // Take the average of all loudness
        avgloudness /= count;

        if (avgloudness > tmpLoudness.get()) {
            tmpLoudness.set(avgloudness);
//            if (key.toString().contains("%")) {
            topArtist.set(key.toString().split("%%")[1]);
//            }
//                System.out.println("Count: "+count+" ; Avg Loudness: "+tmploudness+" ; TOP ARTIST: " + topArtist);

        }


    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(topArtist, tmpLoudness);

    }
}