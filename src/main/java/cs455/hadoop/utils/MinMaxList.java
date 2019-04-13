package cs455.hadoop.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class MinMaxList {
    private int maxSimilar;
    private HashMap<Double, ArrayList<String>> maxSimilarMap;
    private int minSimilar;
    private HashMap<Double, ArrayList<String>> minSimilarMap;
    boolean onlymax;

    public MinMaxList(boolean onlymax) {
        maxSimilar = Integer.MIN_VALUE;
        minSimilar = Integer.MAX_VALUE;
        minSimilarMap = new HashMap<>();
        maxSimilarMap = new HashMap<>();
        this.onlymax = onlymax;
    }
    public HashMap<Double, ArrayList<String>> getMaxMap(){
        return (HashMap<Double, ArrayList<String>>) maxSimilarMap.clone();
    }

    // data = artistName val = length of similar artist list.
    public void updateSimilar(String data, double val) {
        // Update MinSimilar - Unique List
        if (!onlymax) {
            if (val <= minSimilar) {
                if (!minSimilarMap.containsKey(val)) {
                    if (minSimilarMap.size() > 0) {
                        double b = minSimilarMap.keySet().iterator().next();
                        if (b > val) {
                            minSimilarMap.remove(b);
                            ArrayList<String> tmp = new ArrayList<>();
                            tmp.add(data);
                            minSimilarMap.put(val, tmp);
                        }
                    } else {
                        ArrayList<String> tmp = new ArrayList<>();
                        tmp.add(data);
                        minSimilarMap.put(val, tmp);
                    }
                } else {
                    minSimilarMap.get(val).add(data);
                    minSimilarMap.put(val, minSimilarMap.get(val));
                }
            }
        }
        // Update MaxSimilar - Generic Artists
        if (val >= maxSimilar){
            if (!maxSimilarMap.containsKey(val)) {
                if (maxSimilarMap.size() > 0) {
                    double b = maxSimilarMap.keySet().iterator().next();
                    if (b < val) {
                        maxSimilarMap.remove(b);
                        ArrayList<String> tmp = new ArrayList<>();
                        tmp.add(data);
                        maxSimilarMap.put(val, tmp);
                    }
                } else {
                    ArrayList<String> tmp = new ArrayList<>();
                    tmp.add(data);
                    maxSimilarMap.put(val, tmp);
                }
            } else {
                maxSimilarMap.get(val).add(data);
                maxSimilarMap.put(val, maxSimilarMap.get(val));
            }
        }

    }

    private void dumpMapper(Mapper.Context context, HashMap<Double, ArrayList<String>> hashMap, String format) throws
            InterruptedException, IOException{
        if (hashMap.size() > 0) {
            double numericVal = hashMap.keySet().iterator().next();
            for (String item : hashMap.get(numericVal)) {
                if (format.equals("similar")) {
                    context.write(new Text("similar"), new Text(item + "%-%" + numericVal));
                    System.out.println("Similar Min Mapper: " + item + " " + numericVal);
                } else if (format.equals("hot")) {
                    context.write(new Text(item), new Text("hot#-#" + numericVal));
                }

            }
        }
    }

    public void sendFromMapperContext(Mapper.Context context, String format) throws
            InterruptedException, IOException{
            dumpMapper(context, minSimilarMap, format);
            dumpMapper(context, maxSimilarMap, format);
//        if (maxSimilarArtist.size() > 0) {
//            double numericVal = maxSimilarArtist.keySet().iterator().next();
//            for (String item : maxSimilarArtist.get(numericVal)) {
//                if (format.equals("similar")) {
//                    context.write(new Text("similar"), new Text(item + "%-%" + numericVal));
//                System.out.println("Similar Min Mapper: " + item + " " + numericVal);
//            }else if(format.equals("songId")) {
//                context.write(new Text(item), new Text("hot#-#" +numericVal))
//            }
//
//            }
//        }

    }

    public void dumpReducer(Reducer.Context context, HashMap<Double, ArrayList<String>> hashMap, String format)
            throws InterruptedException, IOException{
        if (hashMap.size() > 0) {
            double numericVal = hashMap.keySet().iterator().next();
            for (String item : hashMap.get(numericVal)) {
                context.write(new Text(format + item), new Text(String.valueOf(numericVal)));
                System.out.println(format + " : "+ item + " "+ numericVal);

            }
        }
    }


    public void sendToReducerContext(Reducer.Context context, String format) throws IOException, InterruptedException {
        if (format.equals("similar")) {
            dumpReducer(context, minSimilarMap, "similar#-#Most Unique Artist: ");
            dumpReducer(context, maxSimilarMap, "similar#-#Most generic Artist: ");
        } else if (format.equals("hot")) {
            // SongId + "%%"+songTitle is stored in array in HashMap
            // Need to write- context.write(new Text("hot#-#"+SongId + "%%"+songTitle),new Text(String.valueOf
            // (maxHotness)));
            dumpReducer(context,maxSimilarMap, "hot#-#");
        }
    }

}

