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
            try {
//                System.out.println("VALUE: HotReducer:: "+val);
                String[] splits = val.toString().split("#-#");
                if (splits[0].equals("hot")) {
                    hotness = Double.parseDouble(splits[1]);
                } else if (splits[0].equals("title")) {
                    title = splits[1];
                }
            }catch (Exception e){
                System.out.println("ERROR:  HotReducer: "+val + " == "+key );
            }
        }
//        System.out.println("CHECK:  HotReducer: "+title + " == "+hotness );
        if(title.length() > 0) {
            context.write(new Text(key.toString() + "%%" + title), new DoubleWritable(hotness));
        }

    }
}
