package cs455.singletasks.fading;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class FadeTimeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private static int endOfFadingIdx = 6;
    private static int startOfFadingIdx = 13;
    private static int songIdx = 1;
    private static int durationIdx = 5;

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {


        if (key.get() == 0l && value.toString().contains("song_id")) {
            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0)));
            startOfFadingIdx = header.indexOf("start_of_fade_out");
            songIdx = header.indexOf("song_id");
            endOfFadingIdx = header.indexOf("end_of_fade_in");
            durationIdx = header.indexOf("duration");
        } else {
            String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);
            String songId = items[songIdx].trim();
            String endTime = items[endOfFadingIdx].trim();
            String startTime = items[startOfFadingIdx].trim();
            String durationTime = items[durationIdx].trim();
            double totalFading;
//            if (songId.length() > 0 && endTime.length() > 0 && startTime.length() > 0 && durationTime.length() > 0) {
            double endFading = Double.parseDouble(endTime);
            double startFading = Double.parseDouble(startTime);
            double songDuration = Double.parseDouble(durationTime);
            totalFading = endFading + (songDuration - startFading);
//            System.out.println("Time Mapper:: Song ID= " + songId + " EndFading= " + endTime +
//                    " StartTime= " + startTime + " Duration= " + durationTime + " Fading= " + totalFading);
            context.write(new Text(songId), new Text("fading#-#" + totalFading));

//        }
        }
    }
}
