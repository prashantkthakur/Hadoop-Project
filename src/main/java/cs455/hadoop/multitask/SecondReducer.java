package cs455.hadoop.multitask;

import cs455.hadoop.utils.DoubleComparator;
import cs455.hadoop.utils.MinMaxList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.PriorityQueue;

public class SecondReducer extends Reducer<Text, Text, Text, Text> {

    private int maxCount = Integer.MIN_VALUE;
    private String mostSongArtist = "";
//    private double maxHot = Double.MIN_VALUE;
//    private String maxHotSong = "";
    private double maxFading = Double.MIN_VALUE;
    private String maxFadingSong = "";
    private double maxLength = Double.MIN_VALUE;
    private double minLength = Double.MAX_VALUE;
    private ArrayList<Double> medianLength = new ArrayList<>();
    private HashMap<Double, ArrayList<String>> titleBuffer = new HashMap<>();
    private double loudnessAvg = Double.MIN_VALUE;
    private String loudArtist = "";

    private DoubleComparator customComparator = new DoubleComparator();
    // Create a priority queue to store top 10 songs. PriorityQueue behaves as Min-Heap Map by default.
    private PriorityQueue<String> energySongs = new PriorityQueue<>(10, customComparator);
    private PriorityQueue<String> danceSongs = new PriorityQueue<>(10, customComparator);
    private double maxDanceable = Double.MIN_VALUE;


    private MinMaxList finalHotness = new MinMaxList(true);


    private void addToList(double length, String title) {
        if (!titleBuffer.containsKey(length)) {
            ArrayList<String> tmpArray = new ArrayList<>();
            tmpArray.add(title);
            titleBuffer.put(length, tmpArray);
        } else {
            titleBuffer.get(length).add(title);
        }
    }

    private void updateQueue(PriorityQueue queue, String data){
        queue.add(data);
        System.out.println("DanceReducer:: updateQueue= "+queue.peek());
        if (queue.size() > 10)
            System.out.println("DanceReducer:: Size= "+queue.size()+" higher Poll queue= "+queue.peek());
        queue.poll();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int songCount;
        double fading;
        double hotness;
        double length;
        String hotTitle;
        String fadingTitle;

        if(key.toString().equals("similar")){
            for (Text val: values){
                String[] items = val.toString().split("%-%");
                context.write(new Text(items[0]), new Text(items[1]));
            }

        }

        if (key.toString().equals("count")) {
            for (Text val : values) {
                String[] items = val.toString().split("%-%");
                songCount = Integer.parseInt(items[1]);
                if (songCount > maxCount) {
                    maxCount = songCount;
                    mostSongArtist = items[0].split("%%")[1];
                }
            }
        }

        if (key.toString().equals("hot")) {
            for (Text val : values) {
                String[] items = val.toString().split("%-%");
                hotness = Double.parseDouble(items[1]);
                hotTitle = items[0].split("%%")[1];
                finalHotness.updateSimilar(hotTitle, hotness);
//                if (hotness > maxHot) {
//                    maxHot = hotness;
//                    maxHotSong = hotTitle;
//                    System.out.println("SecondReducer:: Hot title= " + hotTitle + "  Value= " + hotness);
//                }
            }
        }

        if(key.toString().equals("fading")){
            for(Text val : values){
                String[] items = val.toString().split("%-%");
                fading = Double.parseDouble(items[1]);
                fadingTitle = items[0].split("%%")[1];
                if(fading > maxFading){
                    maxFading = fading;
                    maxFadingSong = fadingTitle;
                    System.out.println("SecondReducer:: Fading title= "+fadingTitle + "  Value= "+fading);

                }
            }
        }

        if (key.toString().equals("dance") || key.toString().equals("energy")) {
            for (Text val : values) {
                String[] items = val.toString().split("%-%");
                double danceability = Double.parseDouble(items[1]);
                System.out.println("Dance/Energy : "+items[0]+ " value: "+danceability);
                if (items[0].contains("%")) {
//                    danceTitle = items[0].split("%%")[1];
                    // As the danceTitle is checked to be greater than 0 in firstReducer.
//                    if (danceability > maxDanceable && danceability > 0.0 && danceTitle.length() > 0) {
                    if (danceability > maxDanceable && danceability > 0.0 ) {
                        maxDanceable = danceability;
                        switch (key.toString()) {
                            case "dance":
                                updateQueue(danceSongs, items[0] + "%-%" + danceability);
                                break;
                            case "energy":
                                updateQueue(energySongs, items[0] + "%-%" + danceability);

                                break;
                            default:
                                System.out.println("ERROR: DANCE/ENERGY " + key + " value: " + val);
                                break;
                        }

                    }

                }
            }
        }

        if (key.toString().equals("duration")){
                for (Text val : values) {
                    String[] items = val.toString().split("%-%");
                    length = Double.parseDouble(items[1]);
//                    System.out.println("duration:: "+items[0]);
                    String lengthTitle = items[0].split("%%")[1].trim();
                    if (length > maxLength) {
                        maxLength = length;
                    }
                    if (length < minLength){
                        minLength = length;
                    }
                    medianLength.add(length);
                    addToList(length, lengthTitle);
                }
        }

        if (key.toString().contains("loudest#-#")){
            int count = 0;
            double avgloudness = 0.0;
            // calculate info required for computing average of loudness.
            for (Text val : values) {
//                String[] items = val.toString().split("#-#");
                count++;
                avgloudness += Double.parseDouble(val.toString());

            }
            // Take the average of all loudness
            avgloudness /= count;
            System.out.println("AVG loudness: "+avgloudness + "");

            if (avgloudness > loudnessAvg) {
                loudnessAvg = avgloudness;
                loudArtist = key.toString().split("%%")[1];
                System.out.println("Count: "+count+" ; Avg Loudness: "+avgloudness+" ; TOP ARTIST: " + loudArtist);
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws InterruptedException, IOException {

        // Compute songs count.
        context.write(new Text("Artist with the most songs in the dataset: "),
                new Text(mostSongArtist + "  "+String.valueOf(maxCount)+"\n"));
        context.write(new Text("\nHottest Song :"),new Text("\n-----------------------------\n"));

        finalHotness.dumpReducer(context, finalHotness.getMaxMap(), "Hottest Song: ");
//        context.write(new Text("Hottest Song :"),new Text(maxHotSong + " "+maxHot +"\n"));

        context.write(new Text("\nArtist with the highest total time spent Fading :"),
                new Text(maxFadingSong + " " + maxFading +"\n"));

        context.write(new Text("Top 10 danceable songs"),new Text("\n----------------------------\n"));
        while(danceSongs.size() > 0) {
            String[] tmp = danceSongs.poll().split("%-%");
            context.write(new Text(tmp[0].split("%%")[1]),new Text(tmp[1]));
        }
        context.write(new Text("Top 10 energetic songs"),new Text("\n----------------------------\n"));
        while (energySongs.size() > 0){
            //key.toString()+"%%" + songTitle + "%-%" + energy
            String[] tmp = energySongs.poll().split("%-%");
            context.write(new Text(tmp[0].split("%%")[1]),new Text(tmp[1]));
        }


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
        context.write(new Text ("--------------------Max-Length----------------------\n"),null);
        for (String song:titleBuffer.get(max)){
            context.write(new Text(song), new Text(String.valueOf(max)));
        }
        context.write(new Text ("\n--------------------Min-Length----------------------\n"),null);
        for (String song: titleBuffer.get(min)){
            context.write(new Text(song),new Text(String.valueOf(min)));

        }
        context.write(new Text ("\n--------------------Median-Length----------------------\n"),null);
        for(String title: titleBuffer.get(median)){
            context.write(new Text(title), new Text(String.valueOf(median)));

        }

        context.write(new Text("\nAverage loudest Artist: "), new Text(loudArtist + "\t" + loudnessAvg));


    }
}

