package cs455.hadoop.hottness;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HotReducer extends Reducer<Text, Text, Text, DoubleWritable>  {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String title = "";
        double hotness = 0.0;

        for (Text val : values) {
            // Get different inputs from mapper
            String[] splits = val.toString().split("#-#");
            if (splits[0].equals("hot")){
                hotness = Double.parseDouble(splits[1]);
            }
            else if (splits[0].equals("title")){
                title = splits[1];
            }
        }
        if(title.length() > 0) {
            context.write(new Text(key.toString() + "%%" + title), new DoubleWritable(hotness));
        }

    }
}
