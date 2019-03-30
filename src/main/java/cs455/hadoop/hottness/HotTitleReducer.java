package cs455.hadoop.hottness;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class HotTitleReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable tmpHotness = new DoubleWritable();
    private Text title = new Text();
    private Logger logger = Logger.getLogger(HotJob.class);
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double hotness = 0.0;
        // calculate info required for computing average of loudness.
        try{
        for (DoubleWritable val : values) {
            hotness += val.get();
            if (hotness > tmpHotness.get()) {
                tmpHotness.set(hotness);
                title.set(key.toString().split("%%")[1].trim());

            }
        }

        }catch (Exception e){
            System.err.println("ERROR::: HotTitleReducer:: key= " + key.toString());
            logger.error("HotTitleReducer:: key= " + key.toString());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(title, tmpHotness);

    }
}
