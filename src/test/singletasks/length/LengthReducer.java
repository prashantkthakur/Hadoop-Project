package cs455.singletasks.length;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LengthReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String title = "";
        double duration = 0.0;

        for (Text val : values) {
            try {
//                System.out.println("VALUE: LengthReducer:: " + val);
                String[] splits = val.toString().split("#-#");
                if (splits[0].equals("duration")) {
                    duration = Double.parseDouble(splits[1]);
                } else if (splits[0].equals("title")) {
                    title = splits[1];
                }
            } catch (Exception e) {
                System.out.println("Exception ERROR:  LengthReducer: value= " + val + " key= " + key);
            }
        }
        if (title.length() > 0 && duration > 0.0) {
            context.write(new Text(key.toString() + "%%" + title), new DoubleWritable(duration));
        }

    }
}