package cs455.hadoop.multitask;

import cs455.hadoop.utils.DoubleComparator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

public class FirstReducer extends Reducer<Text, Text, Text, Text> {
    // Song Count
    private int tmpCount = Integer.MIN_VALUE;
    private String mostSongArtist = "";
    private String artistFading = "";
    private double maxFade;
    private DoubleComparator customComparator = new DoubleComparator();
    // Create a priority queue to store top 10 songs. PriorityQueue behaves as Min-Heap Map by default.
    private PriorityQueue<String> energySongs = new PriorityQueue<>(10, customComparator);
    private PriorityQueue<String> danceSongs = new PriorityQueue<>(10, customComparator);
    private double maxDanceable;
    private double maxEnergy;
    private double maxHotness = Double.MIN_VALUE;
    private String hotSongId = "";
    private String songTitle = "";
//    private double maxLoudest = Double.MIN_VALUE;
//    private String maxLoudArtist = "";

    private int minSimilar = Integer.MAX_VALUE;
    private int maxSimilar = Integer.MIN_VALUE;
    HashMap<Integer, ArrayList<String>> maxSimilarArtist = new HashMap<>();
    //    private ArrayList<String> maxSimilarArtist = new ArrayList<>();
    HashMap<Integer, ArrayList<String>> minSimilarArtist = new HashMap<>();




    private void updateQueue(PriorityQueue queue, String data){
        queue.add(data);
        System.out.println("DanceReducer:: updateQueue= "+queue.peek());
        if (queue.size() > 10)
            System.out.println("DanceReducer:: Size= "+queue.size()+" higher Poll queue= "+queue.peek());
        queue.poll();
    }

    private void updateSimilarArtist(Iterable<Text> values) {
        // Update MinSimilar - Unique List
        for (Text item: values) {
            String[] tmplist = item.toString().split("%-%");
            int val = Integer.parseInt(tmplist[1]);
            String data = tmplist[0];
            // Update MaxSimilar - Generic Artists
            if (val >= maxSimilar) {
                if (!maxSimilarArtist.containsKey(val)) {
                    if (maxSimilarArtist.size() > 0) {
                        int b = maxSimilarArtist.keySet().iterator().next();
                        if (b < val) {
                            maxSimilarArtist.remove(b);
                            ArrayList<String> tmp = new ArrayList<>();
                            tmp.add(data);
                            maxSimilarArtist.put(val, tmp);
                        }
                    } else {
                        ArrayList<String> tmp = new ArrayList<>();
                        tmp.add(data);
                        maxSimilarArtist.put(val, tmp);
                    }
                } else {
                    maxSimilarArtist.get(val).add(data);
                    maxSimilarArtist.put(val, maxSimilarArtist.get(val));
                }
            }
            if (val <= minSimilar) {
                if (!minSimilarArtist.containsKey(val)) {
                    if (minSimilarArtist.size() > 0) {
                        int b = minSimilarArtist.keySet().iterator().next();
                        if (b > val) {
                            minSimilarArtist.remove(b);
                            ArrayList<String> tmp = new ArrayList<>();
                            tmp.add(data);
                            minSimilarArtist.put(val, tmp);
                        }
                    } else {
                        ArrayList<String> tmp = new ArrayList<>();
                        tmp.add(data);
                        minSimilarArtist.put(val, tmp);
                    }
                } else {
                    minSimilarArtist.get(val).add(data);
                    minSimilarArtist.put(val, minSimilarArtist.get(val));
                }
            }
        }

    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int songCount = 0;
        // Artist name and ID
        String artistInfo = "";
        double fading = 0.0;
        double loudness = Double.MIN_VALUE;
        double dance = 0.0;
        double energy = 0.0;

        String duration = "";
//        String durationSongId = "";

        if (key.toString().equals("similar")){
                System.out.println("Similar received......................................");
            updateSimilarArtist(values);
        }

        for (Text val : values) {
            // Parse different values for the same song_id
            String[] items = val.toString().trim().split("#-#");
            if (items[0].equals("name")) {
                // Retrieve the artist id and artist name
                //name#-#artistId + "%%" + artistName
                artistInfo = items[1];


            } else if (items[0].equals("title")) {
                // Song title
                songTitle = items[1];

            } else if (items[0].equals("count")) {
                // Songs count
                // count#-#count_value
                songCount += Integer.parseInt(items[1]);
                if (songCount > tmpCount) {
                    tmpCount = songCount;
                    mostSongArtist = key.toString();
                }
                // End song count


            } else if (items[0].equals("fading")) {
                // Fading information
                // fading#-#fading_value
                fading = Double.parseDouble(items[1]);

            } else if (items[0].equals("duration")) {
                // Duration information
                duration = items[1];
//                durationSongId = key.toString();

            } else if (items[0].equals("loudest")) {
                // Loudness information
                // loudest#-#loudness_value
                loudness = Double.parseDouble(items[1]);

            } else if (items[0].equals("hot")) {
                // hotness information
                double hotness = Double.parseDouble(items[1]);
                if (hotness > maxHotness){
                    maxHotness = hotness;
                    hotSongId = key.toString();
                }



            } else if (items[0].equals("dance")) {
                // Danceability information
                String[] danceEnergyItems = items[1].split("%%");
                dance = Double.parseDouble(danceEnergyItems[0]);
                energy = Double.parseDouble(danceEnergyItems[1]);

            } else {
                System.out.println("ERROR:: FirstReducer: Received a non-matching item. " +
                        "Key= " + key.toString() + " Value= " + val);
            }
        }
        // End of iteration on values.

        // Process the value set during the iterations.

        // Start of fading
        if (fading > maxFade && artistInfo.length() > 0) {
            maxFade = fading;
            artistFading = artistInfo;
            System.out.println("FirstReducer:: artistInfo= "+artistInfo + "  Value= "+maxFade);
        }
        // Start loudness
        // Update only if artist info is present for that song.
        if (loudness != Double.MIN_VALUE && artistInfo.length() > 0){
            context.write(new Text("loudest#-#" + artistInfo), new Text(String.valueOf(loudness)));
//            maxLoudest = loudness;
//            maxLoudArtist = artistInfo;
        }
        // Duration
        if (duration.length() > 0 && songTitle.length() > 0) {
            context.write(new Text("duration#-#" + key.toString() + "%%" + songTitle), new Text(duration));
        }

        // Start of dance & enegry
        if (dance > maxDanceable && dance > 0.0 && songTitle.length() > 0) {
//        if (dance > maxDanceable  && songTitle.length() > 0) {
            maxDanceable = dance;
            System.out.println("DanceReducer:: maxDance =" + songTitle + "%-%" + dance);
            updateQueue(danceSongs, key.toString()+"%%" + songTitle + "%-%" + dance);

        }
        if (energy > maxEnergy && energy > 0.0 && songTitle.length() > 0) {
//        if (energy > maxEnergy  && songTitle.length() > 0) {
            maxEnergy = energy;
            System.out.println("DanceReducer:: maxEnergy =" + songTitle + "%-%" + energy);
            updateQueue(energySongs, key.toString()+"%%" + songTitle + "%-%" + energy);
        }
        // End of dance & energy

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Write the final value for the song count for portion of keys (song_id)
        context.write(new Text("count#-#"+mostSongArtist), new Text(String.valueOf(tmpCount)));
        // Write the max Fading in this portion of keys.
        context.write(new Text("fading#-#"+artistFading), new Text(String.valueOf(maxFade)));

        // Write to context top 10 danceable and energetic songs.
//        context.write(new Text("dance#-#Top 10 danceable songs"),new Text("7.7"));
        while(danceSongs.size() > 0) {
            String[] tmp = danceSongs.poll().split("%-%");
            context.write(new Text("dance#-#"+tmp[0]),new Text(tmp[1]));
        }
//        context.write(new Text("energy#-#Top 10 energetic songs"),new Text("7.7"));
        while (energySongs.size() > 0){
            //key.toString()+"%%" + songTitle + "%-%" + energy
            String[] tmp = energySongs.poll().split("%-%");
            context.write(new Text("energy#-#"+tmp[0]),new Text(tmp[1]));
        }
        // Hottest song in this batch
        context.write(new Text("hot#-#"+hotSongId + "%%"+songTitle),new Text(String.valueOf(maxHotness)));

        if(minSimilarArtist.size() >0) {
            int tmpKey = minSimilarArtist.keySet().iterator().next();
            for (String val: minSimilarArtist.get(tmpKey)) {
                context.write(new Text("similar#-#Most Unique Artist: " + val), new Text(String.valueOf(tmpKey)));
                System.out.println("Similar Min: "+ val + " "+ tmpKey);
            }
        }
        if (maxSimilarArtist.size() > 0) {
            int tmpKey = maxSimilarArtist.keySet().iterator().next();
            for (String val: maxSimilarArtist.get(tmpKey)) {
                context.write(new Text("similar#-#Most generic Artist: " + val), new Text(String.valueOf(tmpKey)));
                System.out.println("Similar Max: "+ val + " "+ tmpKey);
            }
        }


    }
}