package cs455.singletasks.loudest;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Mapper: Reads line by line, retrieve the key from header. Emit <artist_id+artist_name, 1> pairs.
 */
public class LoudestMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static int songIdx = 0;
    private static int loudIdx = 0;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the header from the csv
        if (key.get() == 0l && value.toString().contains("song_id")) {
            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0)));
            songIdx = header.indexOf("song_id");
            loudIdx = header.indexOf("loudness");
        } else {
            String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);
            String songId = items[songIdx].trim();
            String loudness = items[loudIdx].trim();

            context.write(new Text(songId), new Text("loudest#-#"+loudness));

        }
    }
}
