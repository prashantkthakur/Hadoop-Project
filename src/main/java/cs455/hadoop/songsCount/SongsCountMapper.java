package cs455.hadoop.songsCount;

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
public class SongsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the header from the csv
        int artistIdx = 0;
        int nameIdx = 0;
//        int songIdx = 0;
        if (key.get() == 0) {
            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",")));
            artistIdx = header.indexOf("artist_id");
            nameIdx = header.indexOf("artist_name");
//            songIdx = header.indexOf("song_id");

        } else {
            String[] items = value.toString().split(",");
            String artistId = items[artistIdx];
            String artistName = items[nameIdx];
//            String songId = value.toString().split(",")[songIdx];
            // emit word, count pairs.
            context.write(new Text(artistId +"%" + artistName), new IntWritable(1));
        }
    }
}
