package cs455.hadoop.avgtask;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

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

public class AvgAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

    private int startTimeIdx = 6;
    private int pitchIdx = 20;
    private int timberIdx = 21;
    private int maxLoudnessIdx = 22;
    private int maxLoudnessTimeIdx = 23;
    private int startLoudnessIdx = 24;
    private int segIdx = 18;
    private long counter = 0;

    private double startTime;
    private String[] pitch;
    private String[] timber;
    private String[] maxLoudness;
    private String[] maxLoudnessTime;
    private String[] startLoudness;

    private ArrayList<Double> resultPitch = new ArrayList<>();
    private ArrayList<Double> resultTimber = new ArrayList<>();
    private ArrayList<Double> resultMaxLoudness = new ArrayList<>();
    private ArrayList<Double> resultMaxLoudnessTime = new ArrayList<>();
    private ArrayList<Double> resultStartLoudness = new ArrayList<>();

    private void addValues(ArrayList<Double>result, String[] list){
            for (int i=0; i < list.length; ++i){
                try{
                    result.set(i, result.get(i) + Double.parseDouble(list[i]));
                }catch (IndexOutOfBoundsException aib){
                    result.add(Double.parseDouble(list[i]));
                }
            }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0l && value.toString().contains("song_id")) {
            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0)));
            startTimeIdx = header.indexOf("end_of_fade_in");
            pitchIdx = header.indexOf("segments_pitches");
            timberIdx = header.indexOf("segments_timbre");
            maxLoudnessIdx = header.indexOf("segments_loudness_max");
            maxLoudnessTimeIdx = header.indexOf("segments_loudness_max_time");
            startLoudnessIdx = header.indexOf("segments_loudness_start");
            segIdx = header.indexOf("segments_start");
            System.out.printf("Index: staratTime=%s, pitch=%s, timber=%s, maxLoudness=%s, mlt=%s, startloud=%s, " +
                            "seg=%s\n",
                    startTimeIdx, pitchIdx, timberIdx, maxLoudnessIdx, maxLoudnessTimeIdx, startLoudnessIdx,segIdx);

        }else{
            counter++;
            String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);
            startTime += Double.parseDouble(items[startTimeIdx]);
            pitch = items[pitchIdx].split("\\s+");
            timber = items[timberIdx].split("\\s+");
            maxLoudness = items[maxLoudnessIdx].split("\\s+");
            maxLoudnessTime = items[maxLoudnessTimeIdx].split("\\s+");
            startLoudness = items[startLoudnessIdx].split("\\s+");
            String[] seg = items[segIdx].split(" ");
            System.out.printf("Avg len: pitch=%s, timber=%s, maxloud=%s, maxloudtime=%s, startloud=%s segment=%s\n",
                    pitch.length, timber.length, maxLoudness.length, maxLoudnessTime.length, startLoudness.length,
                    seg.length);
            // Do element wise addition of two results.
            addValues(resultPitch, pitch);
            addValues(resultMaxLoudness, maxLoudness);
            addValues(resultTimber, timber);
            addValues(resultMaxLoudnessTime, maxLoudnessTime);
            addValues(resultStartLoudness, startLoudness);
        }

//        double result[] = new double[pitch.length]; //default initialize to 0.0



    }

    @Override
    protected void cleanup(Context context) throws InterruptedException, IOException {
        // Send the sum and corresponding data.
        context.write(new Text("avg"), new Text("start-time#-#"+startTime));
        context.write(new Text("avg"), new Text("counter#-#"+counter));
        String tmp = resultPitch.stream().map(Object::toString).collect(Collectors.joining(" "));
//        String pitch = String.join(" ", tmp);
        context.write(new Text("avg"), new Text("pitch#-#"+tmp));

        tmp = resultTimber.stream().map(Object::toString).collect(Collectors.joining(" "));
        context.write(new Text("avg"), new Text("timber#-#"+tmp));

        tmp = resultMaxLoudness.stream().map(Object::toString).collect(Collectors.joining(" "));
        context.write(new Text("avg"), new Text("max-loud#-#"+tmp));

        tmp = resultMaxLoudnessTime.stream().map(Object::toString).collect(Collectors.joining(" "));
        context.write(new Text("avg"), new Text("loud-time#-#"+tmp));

        tmp = resultStartLoudness.stream().map(Object::toString).collect(Collectors.joining(" "));
        context.write(new Text("avg"), new Text("start-loud#-#"+tmp));

    }

}
