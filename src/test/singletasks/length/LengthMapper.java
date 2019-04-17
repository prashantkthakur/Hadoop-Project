package cs455.singletasks.length;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class LengthMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static int durationIdx = 5;
    private static int songIdx = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the header from the csv

        if (key.get() == 0l && value.toString().contains("song_id")) {
            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0)));
            durationIdx = header.indexOf("duration");
            songIdx = header.indexOf("song_id");
        } else {
            String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);
            String songId = items[songIdx].trim();
            String durationId = items[durationIdx].trim();
            context.write(new Text(songId), new Text("duration#-#" + durationId));

        }
    }
}