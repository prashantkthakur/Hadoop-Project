package cs455.singletasks.hottness;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class HotMapper extends Mapper<LongWritable, Text, Text, Text> {

    // TODO- Somehow these value are not initialized from header.
    // Solution: The file is large so header is not present in all the chunk so initialized to default value.
    // Made it static so that it is one for the entire class so that it can be reused.
    private static int hotnessIdx = 0;
    private static int songIdx = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        if (key.get() == 0l && value.toString().contains("song_id")) {
            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0)));
            hotnessIdx = header.indexOf("song_hotttnesss");
            songIdx = header.indexOf("song_id");
//            System.out.println("HotMapper:: Header "+ hotnessIdx + " : "+songIdx);

        } else {
            String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);

            String songId = items[songIdx].trim();
            String hotId = items[hotnessIdx].trim();

            if (songId.length() > 0 && hotId.length() > 0) {
//                if (count++ % 10000 == 0) {
//                    System.out.println(songIdx + "::" + hotnessIdx + "PKT:: HOTMAPPER :: Hot= " + items[2] + " :: " +
//                            "Song ID= " + items[1]);
//                    System.out.println("PKT:: HOT:: " + hotId + " Song ID:  " + songId);
//                }
                context.write(new Text(songId), new Text("hot#-#" + hotId));
            }
        }
    }
}