package cs455.hadoop.utils;

import java.util.ArrayList;
import java.util.HashMap;

public class Utilities {
    private int maxSimilar = Integer.MIN_VALUE;
    HashMap<Integer, ArrayList<String>> maxSimilarArtist = new HashMap<>();
    //    private ArrayList<String> maxSimilarArtist = new ArrayList<>();
    private int minSimilar = Integer.MAX_VALUE;
    HashMap<Integer, ArrayList<String>> minSimilarArtist = new HashMap<>();

    public void updateSimilarArtist(String data, int val) {
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
        // Update MaxSimilar - Generic Artists
        else if (val >= maxSimilar){
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

    }
}
