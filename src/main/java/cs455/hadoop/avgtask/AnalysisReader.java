package cs455.hadoop.avgtask;

import cs455.hadoop.utils.MinMaxList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

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
public class AnalysisReader extends Mapper<LongWritable, Text, Text, Text> {
    private int songIdx = 1;
    private int danceIdx = 4;
    private int energyIdx = 7;
    private int loudnessIdx = 10;
    private int endFadeIdx = 6;
    private int startFadeIdx = 13;
    private int durationIdx = 5;
    private int hotIdx = 2;
    private int songKeyIdx = 8;
    private int tempoIdx = 14;
    private int modeIdx = 11;


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the header from the csv

        if (key.get() == 0l && value.toString().contains("song_id")) {

        } else {
            String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);
            String songId = items[songIdx].trim();
            String dance = items[danceIdx].trim();
            String energy = items[energyIdx].trim();
            String loudness = items[loudnessIdx].trim();
            String hotness = items[hotIdx].trim();
            String endTime = items[endFadeIdx].trim();
            String startTime = items[startFadeIdx].trim();
            String durationTime = items[durationIdx].trim();
            String songKey = items[songKeyIdx].trim();



        }
    }
}
