package cs455.singletasks.hottness;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HotTitleReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable tmpHotness = new DoubleWritable();
    private Text title = new Text();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double hotness;
//        System.out.println("HotTitleReducer:: "+ key.toString());
        // calculate info required for computing average of loudness.
        try{
            for (DoubleWritable val : values) {
//                System.out.println("HotTitleReducer:: "+ val);
                hotness = val.get();
                if (hotness > tmpHotness.get()) {
                    tmpHotness.set(hotness);
                    title.set(key.toString().split("%%")[1].trim());
                    System.out.println("HotTitleReducer:: title= "+title + "  Value= "+tmpHotness);
                }
            }
        }catch (Exception e){
            System.err.println("ERROR::: HotTitleReducer:: key= " + key.toString());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(title, tmpHotness);

    }
}
