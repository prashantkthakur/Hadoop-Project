package cs455.hadoop.loudest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;



/**
 * Mapper: Reads line by line, retrieve the key from header. Emit <artist_id+artist_name, 1> pairs.
 */
public class ArtistMapper extends Mapper<LongWritable, Text, Text, Text> {
    private int nameIdx = 0;
    private int songIdx = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the header from the csv

        if (key.get() == 0l) {
            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0)));
            nameIdx = header.indexOf("artist_name");
            songIdx = header.indexOf("song_id");
        } else {
            String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);
            String songId = items[songIdx].trim();
            String artistName = items[nameIdx].trim();

            context.write(new Text(songId), new Text("name\t"+artistName));

        }
    }
}