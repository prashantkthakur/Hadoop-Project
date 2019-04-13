package cs455.hadoop.multitask;

import cs455.hadoop.utils.MinMaxList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/** Index of header
 * 0 Item:
 * 1 Item: song_id
 * 2 Item: song_hotttnesss
 * 3 Item: analysis_sample_rate
 * 4 Item: danceability
 * 5 Item: duration
 * 6 Item: end_of_fade_in
 * 7 Item: energy
 * 8 Item: key
 * 9 Item: key_confidence
 * 10 Item: loudness
 * 11 Item: mode
 * 12 Item: mode_confidence
 * 13 Item: start_of_fade_out
 * 14 Item: tempo
 * 15 Item: time_signature
 * 16 Item: time_signature_confidence
 * 17 Item: track_id
 * 18 Item: segments_start
 * 19 Item: segments_confidence
 * 20 Item: segments_pitches
 * 21 Item: segments_timbre
 * 22 Item: segments_loudness_max
 * 23 Item: segments_loudness_max_time
 * 24 Item: segments_loudness_start
 * 25 Item: sections_start
 * 26 Item: sections_confidence
 * 27 Item: beats_start
 * 28 Item: beats_confidence
 * 29 Item: bars_start
 * 30 Item: bars_confidence
 * 31 Item: tatums_start
 * 32 Item: tatums_confidence
 *
 */
public class AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static int songIdx = 1;
    private static int danceIdx = 4;
    private static int energyIdx = 7;
    private static int loudnessIdx = 10;
    private static int endFadeIdx = 6;
    private static int startFadeIdx = 13;
    private static int durationIdx = 5;
    private static int hotIdx = 2;

    private double totalFading;

    private MinMaxList songHottness = new MinMaxList(true);



    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the header from the csv

        if (key.get() == 0l && value.toString().contains("song_id")) {
            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0)));
            danceIdx = header.indexOf("danceability");
            energyIdx = header.indexOf("energy");
            songIdx = header.indexOf("song_id");
            loudnessIdx = header.indexOf("loudness");
            endFadeIdx = header.indexOf("end_of_fade_in");
            startFadeIdx = header.indexOf("start_of_fade_out");
            durationIdx = header.indexOf("duration");
            hotIdx = header.indexOf("song_hotttnesss");
            System.out.println("Header indexes computed...");

        } else {
            String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);
            String songId = items[songIdx].trim();
            double dance = Double.parseDouble(items[danceIdx].trim());
            double energy = Double.parseDouble(items[energyIdx].trim());
            String loudness = items[loudnessIdx].trim();
            String hotness = items[hotIdx].trim();
            String endTime = items[endFadeIdx].trim();
            String startTime = items[startFadeIdx].trim();
            String durationTime = items[durationIdx].trim();

            try{
                double endFading = Double.parseDouble(endTime);
                double startFading = Double.parseDouble(startTime);
                double songDuration = Double.parseDouble(durationTime);
                totalFading = endFading + (songDuration - startFading);
                // Send fading information for each song_id
                context.write(new Text(songId), new Text("fading#-#"+totalFading));

            }catch (NumberFormatException nfe){
                System.out.println("Error converting fading to Double.");
            }

            // Send duration information
            context.write(new Text(songId), new Text("duration#-#" + durationTime));

            // Send loudness information
            context.write(new Text(songId), new Text("loudest#-#"+loudness));

            // Send hottness information.
            if (songId.length() > 0 && hotness.length() > 0) {
                System.out.println("Hottest: "+songId + " "+hotness);
                songHottness.updateSimilar(songId, Double.parseDouble(hotness));
//                context.write(new Text(songId), new Text("hot#-#" + hotness));
            }

            // Remove unrated songs from being processed.
            if (dance > 0 && energy > 0) {
                context.write(new Text(songId), new Text("dance#-#" + dance + "%%" + energy));
            }

        }
    }
    // Don't need as songId is unique in the data
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        songHottness.sendFromMapperContext(context, "hot");

    }
}