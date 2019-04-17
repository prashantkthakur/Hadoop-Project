package cs455.singletasks.hottness;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class TitleMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static int titleIdx = 0;
    private static int songIdx = 0;

    @Override

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (key.get() == 0l && value.toString().contains("song_id")) {
            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0)));
            titleIdx = header.indexOf("title");
            songIdx = header.indexOf("song_id");
        } else {
            String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);
            String songId = items[songIdx].trim();
            String title = items[titleIdx].trim();
            if (songId.length() > 0 && title.length() > 0) {
                context.write(new Text(songId), new Text("title#-#" + title));
            }

        }
    }
}
