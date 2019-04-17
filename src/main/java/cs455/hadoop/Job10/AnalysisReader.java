package cs455.hadoop.Job10;

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

/**
 * 7. Create segment data for the average song.
 * Include start time, pitch, timbre, max loudness, max loudness time, and start loudness.
 */

public class AnalysisReader extends Mapper<LongWritable, Text, Text, Text> {

    private int songIdx = 1;
    private int endFadeIdx = 6;



    /**
     * Indices for Q9
     */
    private int tempoIdx = 14;
    private int songKeyIdx = 8;
    private int durationIdx = 5;
    private int modeIdx = 11;
    private int loudnessIdx = 10;
    private int startFadeIdx = 13;
    private int hottnessIdx = 2;



    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (key.get() == 0l && value.toString().contains("song_id")) {
//            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0)));
//            startTimeIdx = header.indexOf("end_of_fade_in");
//            pitchIdx = header.indexOf("segments_pitches");
//            timberIdx = header.indexOf("segments_timbre");
//            maxLoudnessIdx = header.indexOf("segments_loudness_max");
//            maxLoudnessTimeIdx = header.indexOf("segments_loudness_max_time");
//            startLoudnessIdx = header.indexOf("segments_loudness_start");

        } else {
            String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);

            // Data Prepration..
            String outSongId = items[songIdx].trim();
//            String dance = items[danceIdx].trim();
//            String energy = items[energyIdx].trim();
            String outLoudness = items[loudnessIdx].trim();
            String outHotness = items[hottnessIdx].trim();
            String endTime = items[endFadeIdx].trim();
            String outStartFade = items[startFadeIdx].trim();
            String outDurationTime = items[durationIdx].trim();
            String outTempo = items[tempoIdx].trim();
            String outMode = items[modeIdx].trim();
            String outSongKey = items[songKeyIdx].trim();
            String outVal = outHotness + "," + outDurationTime + "," + endTime + "," + outSongKey + "," + outLoudness + "," + outMode +
                    "," + outStartFade + "," + outTempo;
            if (outHotness.length() > 0) {
                double tmpHot = Double.parseDouble(outHotness);
                if (tmpHot > 0.0) {
                    if (tmpHot > 0.5) {
                        outVal += ",GOOD";
                    } else {
                        outVal += ",BAD";
                    }
                    context.write(new Text(outSongId), new Text("analysis#-#" + outVal));
                }
            }
        }
    }


}
