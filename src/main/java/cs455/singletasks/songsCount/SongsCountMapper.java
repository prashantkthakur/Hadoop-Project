package cs455.singletasks.songsCount;

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
    private static int artistIdx = 0;
    private static int nameIdx = 0;
    //       private int songIdx = 0;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the header from the csv

        if (key.get() == 0l && value.toString().contains("artist_id")) {
            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0)));
            artistIdx = header.indexOf("artist_id");
            nameIdx = header.indexOf("artist_name");
//            songIdx = header.indexOf("song_id");
        } else {
                String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);
                String artistId = items[artistIdx].trim();
                String artistName = items[nameIdx].trim();
                if (artistId.length() < 1 || artistName.length() < 1)
                    System.out.println("Mapper error: length less"+ artistId + ": "+artistName);
//            String songId = value.toString().split(",")[songIdx];
                // emit word, count pairs.
                context.write(new Text(artistId + "%%" + artistName), new IntWritable(1));

        }
    }
}
