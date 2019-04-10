package cs455.singletasks.danceable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs455.hadoop.utils.DoubleComparator;
import java.io.IOException;
import java.util.PriorityQueue;

public class DanceReducer extends Reducer<Text, Text, Text, NullWritable> {
    private DoubleWritable maxDanceable = new DoubleWritable(Double.MIN_VALUE);
    private DoubleWritable maxEnergy = new DoubleWritable(Double.MIN_VALUE);
    private DoubleComparator customComparator = new DoubleComparator();
    // Create a priority queue to store top 10 songs. PriorityQueue behaves as Min-Heap Map by default.
    private PriorityQueue<String> energySongs = new PriorityQueue<>(10, customComparator);
    private PriorityQueue<String> danceSongs = new PriorityQueue<>(10, customComparator);

    private void updateQueue(PriorityQueue queue, String data){
        queue.add(data);
        System.out.println("DanceReducer:: updateQueue= "+queue.peek());
        if (queue.size() > 10)
            System.out.println("DanceReducer:: Size= "+queue.size()+" higher Poll queue= "+queue.peek());
        queue.poll();
    }
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String title = "";
        double dance = 0.0;
        double energy = 0.0;

        for (Text val : values) {
            try {
//                System.out.println("VALUE: LengthReducer:: " + val);
                String[] splits = val.toString().split("#-#");
                if (splits[0].equals("dance")) {
                    String[] items = splits[1].split("%%");
                    dance = Double.parseDouble(items[0]);
                    energy = Double.parseDouble(items[1]);
                } else if (splits[0].equals("title")) {
                    title = splits[1];
                }
                System.out.println("DanceReducer:: Danceable ="+title+"%%"+dance);
                if (dance > maxDanceable.get() && dance > 0.0 && title.length() > 0){
                    maxDanceable.set(dance);
                    System.out.println("DanceReducer:: maxDanceable ="+title+"%%"+dance);
                    updateQueue(danceSongs, title+"%%"+dance);

                }
                System.out.println("DanceReducer:: Energy ="+title+"%%"+energy);
                if (energy > maxEnergy.get() && energy > 0.0 && title.length() > 0){
                    maxEnergy.set(energy);
                    System.out.println("DanceReducer:: maxEnergy ="+title+"%%"+energy);
                    updateQueue(energySongs, title+"%%"+energy);

                }
            } catch (Exception e) {
                System.out.println("Exception ERROR:  LengthReducer: value= " + val + " key= " + key);
            }
        }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Top 10 danceable songs\n------------------------------\n\n"),null);
        while(danceSongs.size() > 0) {
            context.write(new Text(danceSongs.poll()),null);
        }
        context.write(new Text("\n\nTop 10 energetic songs\n------------------------------\n\n"),null);
        while (energySongs.size() > 0){
            context.write(new Text(energySongs.poll()),null);
        }
    }
}