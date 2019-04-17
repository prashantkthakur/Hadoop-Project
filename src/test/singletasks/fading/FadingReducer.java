package cs455.singletasks.fading;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FadingReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String idName = "";
        double fading = 0.0;
        for (Text val : values) {
            // Get different inputs from mapper
            String[] splits = val.toString().split("#-#");
            if (splits[0].equals("fading")){
                fading = Double.parseDouble(splits[1]);
            }
            else if (splits[0].equals("name")){
                idName = splits[1];
            }
        }
        if(idName.length() > 0) {
            context.write(new Text(idName), new DoubleWritable(fading));
        }

    }
}