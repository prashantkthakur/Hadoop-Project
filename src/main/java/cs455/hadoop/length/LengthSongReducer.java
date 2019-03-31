package cs455.hadoop.length;

import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class LengthSongReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable maxLength = new DoubleWritable(Double.MIN_VALUE);
    private DoubleWritable minLength = new DoubleWritable(Double.MAX_VALUE);
//    private ArrayList<Text> minSongs = new ArrayList<>();
//    private HashMap<Double, ArrayList<String>> maxSongs = new HashMap<>();
//    private HashMap<Double, ArrayList<String>> minSongs = new HashMap<>();
//    private ArrayList<Text> maxSongs = new ArrayList<>();
    private ArrayList<Double> medianLength = new ArrayList<>();
    private HashMap<Double, ArrayList<String>> titleBuffer = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double length;
        String title;

//        System.out.println("HotTitleReducer:: "+ key.toString());
        // calculate info required for computing average of loudness.
        try{
            for (DoubleWritable val : values) {
                title = key.toString().split("%%")[1].trim();
                length = val.get();
                if (length > maxLength.get()) {
                    maxLength.set(length);
//                    System.out.println("LengthSongReducer:: MaxLen = "+length);
//                    maxSongs.add(new Text(title + "\t"+length));
//                    setData(maxLength.get(), title, maxSongs);
                }
                if (length < minLength.get()){
                    minLength.set(length);
//                    System.out.println("LengthSongReducer:: MinLen = "+length);
//                    minSongs.add(new Text(title + "\t"+length));
//                    setData(minLength.get(), title, minSongs);

                }
                medianLength.add(length);
                setData(length, title, titleBuffer);
            }
        }catch (Exception e){
            System.err.println("ERROR::: LengthSongReducer:: key= " + key.toString());
        }
    }

    private void setData(double length, String title, HashMap<Double, ArrayList<String>> titleBuffer) {
        if (!titleBuffer.containsKey(length)) {
            ArrayList<String> tmp = new ArrayList<>();
            tmp.add(title);
            titleBuffer.put(length, tmp);
        }else{
            titleBuffer.get(length).add(title);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        // Calculate Median length.
        Collections.sort(medianLength);
        double median;
        double max = medianLength.get(medianLength.size()-1);
        double min = medianLength.get(0);

        int middle = medianLength.size()/2;

        if (medianLength.size() % 2 == 1) {
            median = medianLength.get(middle);
        } else {
            median = (medianLength.get(middle-1) + medianLength.get(middle)) / 2.0;
        }
        context.write(new Text ("--------------------Max-Length----------------------\n\n"),null);
        for (String song:titleBuffer.get(max)){
            context.write(new Text(song), new DoubleWritable(max));
        }
        context.write(new Text ("\n\n--------------------Min-Length----------------------\n\n"),null);
        for (String song: titleBuffer.get(min)){
            context.write(new Text(song),new DoubleWritable(min));

        }
        context.write(new Text ("\n\n--------------------Median-Length----------------------\n\n"),null);
        for(String title: titleBuffer.get(median)){
            context.write(new Text(title), new DoubleWritable(median));

        }

    }

}
