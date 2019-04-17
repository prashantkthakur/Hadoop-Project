package cs455.singletasks.danceable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class DanceMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static int danceIdx = 4;
    private static int energyIdx = 7;
    private static int songIdx = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the header from the csv

        if (key.get() == 0l && value.toString().contains("song_id")) {
            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0)));
            danceIdx = header.indexOf("danceability");
            energyIdx = header.indexOf("energy");
            songIdx = header.indexOf("song_id");
        } else {
            String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);
            String songId = items[songIdx].trim();
            double dance = Double.parseDouble(items[danceIdx].trim());
            double energy = Double.parseDouble(items[energyIdx].trim());
            // Remove unrated songs from being processed.
            System.out.println("DanceMapper:: read data= "+"dance#-#" + dance + "%%" + energy);
            if (dance > 0 && energy > 0) {
                context.write(new Text(songId), new Text("dance#-#" + dance + "%%" + energy));
            }

        }
    }
}