package cs455.hadoop.multitask;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/** Index of header
 * 0 Item:
 * 1 Item: artist_familiarity
 * 2 Item: artist_hotttnesss
 * 3 Item: artist_id
 * 4 Item: artist_latitude
 * 5 Item: artist_longitude
 * 6 Item: artist_location
 * 7 Item: artist_name
 * 8 Item: song_id
 * 9 Item: title
 * 10 Item: similar_artists
 * 11 Item: artist_terms
 * 12 Item: artist_terms_freq
 * 13 Item: artist_terms_weight
 * 14 Item: year
 *
 */
public class MetaDataMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static int songIdx = 8;
    private static int nameIdx = 7;
    private static int idIdx = 3;
    private static int titleIdx = 9;
    private static int similarIdx = 10;
    private int maxSimilar = Integer.MIN_VALUE;
    HashMap<Integer, ArrayList<String>> maxSimilarArtist = new HashMap<>();
//    private ArrayList<String> maxSimilarArtist = new ArrayList<>();
    private int minSimilar = Integer.MAX_VALUE;
    HashMap<Integer, ArrayList<String>> minSimilarArtist = new HashMap<>();
//    private ArrayList<String> minSimilarArtist = new ArrayList<>();


    private HashMap<String, SongCount> songStat = new HashMap<>();

    private class SongCount{
        private String songId;
        private int count = 0;
        SongCount(String id, int val){
            this.songId = id;
            this.count = val;
        }

        synchronized SongCount incrementCount(){
            this.count++;
            return this;
        }
        public String getSongId(){
            return songId;
        }
        public int getCount(){return count;}
    }

    private void updateSongsStat(String songId, String data){
        if (!songStat.containsKey(data)){
            songStat.put(data, new SongCount(songId, 1));
        }else{
            songStat.put(data, songStat.get(data).incrementCount());
        }
    }

    private void updateSimilarArtist(String data, int val) {

        // Update MaxSimilar - Generic Artists
        if (val >= maxSimilar){
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
        // Update MinSimilar - Unique List
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

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the header from the csv

        if (key.get() == 0l && value.toString().contains("song_id")) {
            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0)));
            idIdx = header.indexOf("artist_id");
            nameIdx = header.indexOf("artist_name");
            songIdx = header.indexOf("song_id");
            titleIdx = header.indexOf("title");
            similarIdx = header.indexOf("similar_artists");

        } else {
            String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);
            String songId = items[songIdx].trim();
            String artistName = items[nameIdx].trim();
            String artistId = items[idIdx].trim();
            String title = items[titleIdx].trim();
            String similarArtist = items[similarIdx].trim();
            String[] similarList = similarArtist.split("\\s+");
            System.out.println("Similar array length : "+similarList.length);
            // Aggregate the count of songs for one artist
            String info = artistId + "%%" + artistName;
            updateSongsStat(songId, info);
            updateSimilarArtist(artistName, similarList.length);
/**
            if (similarList.length >= maxSimilar){
                maxSimilar = similarList.length;s
//                maxSimilarArtist = artistId +"%%"+artistName;
                maxSimilarArtist.add(artistName + "%-%"+maxSimilar);
                System.out.println("Similar Max: "+maxSimilarArtist + " " + maxSimilar);
            }else if(similarList.length <= minSimilar){
                minSimilar = similarList.length;
//                minSimilarArtist = artistId + "%%" + artistName;
                minSimilarArtist.add(artistName+"%-%"+minSimilar);
                System.out.println("Similar Min: "+minSimilarArtist + " " + minSimilar);

            }
 */
            // Send artist name. Artist name are not unique so added artist_id with it.
            // Needed for :-  loudest song, fading,
            context.write(new Text(songId), new Text("name#-#"+info));

            // Send song title information.
            // Needed for :- hottest song, length of songs.
            if (songId.length() > 0 && title.length() > 0) {
                context.write(new Text(songId), new Text("title#-#" + title));
            }

        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Send out songs count after merging all the information
        for (String artistId: songStat.keySet()) {
            // count#-#artistID%%artistName%%count
            String outVal = "count#-#"+songStat.get(artistId).getCount();
//            context.write(new Text(songStat.get(artistId).getSongId()), new Text(outVal));
            context.write(new Text(artistId), new Text(outVal));
        }
        if (minSimilarArtist.size() > 0) {
            int tmpKey = minSimilarArtist.keySet().iterator().next();
            for (String val : minSimilarArtist.get(tmpKey)) {
                context.write(new Text("similar"), new Text(val + "%-%" + tmpKey));
                System.out.println("Similar Min Mapper: " + val + " " + tmpKey);

            }
        }
        if (maxSimilarArtist.size() > 0) {
            int tmpKey = maxSimilarArtist.keySet().iterator().next();
            for (String item : maxSimilarArtist.get(tmpKey)) {
                context.write(new Text("similar"), new Text(item + "%-%" + tmpKey));
                System.out.println("Similar Max Mapper: " + item + " " + tmpKey);

            }
        }

    }
}