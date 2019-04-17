package cs455.hadoop.Job10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.stream.Collectors;

public class FinalReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void setup(Context context) throws IOException,InterruptedException{
        // Analysis: outHotness+","+outDurationTime+","+endTime+","+outSongKey+","+outLoudness+","+outMode+
        // ","+outStartFade + ","+outTempo
        String header = "artist_familiarity,artist_hotttnesss,year,artist_longitude,song_hotttnesss,duration,end_of_fade_in,key," +
                "loudness,mode,start_of_fade_out,tempo,popularity";
        context.write(new Text(header), null);
    }


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // SongId received as key.
        String analysis = "";
        String meta = "";
        for (Text val : values) {
            String[] items = val.toString().split("#-#");
            if (items[0].equals("analysis")) {
                analysis = items[1];
            }
            if (items[0].equals("meta")) {
                meta = items[1];
            }
        }
        if (meta.length() > 0 && analysis.length() > 0) {
            String outVal = meta + "," + analysis;
            context.write(new Text(outVal), null);
        }


    }


}
