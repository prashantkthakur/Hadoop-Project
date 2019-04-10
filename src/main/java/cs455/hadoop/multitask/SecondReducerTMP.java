//package cs455.hadoop.multitask;
//
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//
//public class SecondReducerTMP extends Reducer<Text, Text, Text, Text> {
//
//    private int maxCount = Integer.MIN_VALUE;
//    private String mostSongArtist = "";
//    private double maxHot = Double.MIN_VALUE;
//    private String maxHotSong = "";
//    private double maxFading = Double.MIN_VALUE;
//    private String maxFadingSong = "";
//    private double maxLength = Double.MIN_VALUE;
//    private double minLength = Double.MAX_VALUE;
//    private ArrayList<Double> medianLength = new ArrayList<>();
//    private HashMap<Double, ArrayList<String>> titleBuffer = new HashMap<>();
//    private double loudnessAvg = Double.MIN_VALUE;
//    private String loudArtist = "";
//    private int loudCount = 0;
//    private double avgloudness = 0.0;
//
//    private void addToList(double length, String title) {
//        if (!titleBuffer.containsKey(length)) {
//            ArrayList<String> tmpArray = new ArrayList<>();
//            tmpArray.add(title);
//            titleBuffer.put(length, tmpArray);
//        }else{
//            titleBuffer.get(length).add(title);
//        }
//    }
//
//    @Override
//    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//
//
//        int songCount = 0;
//        double fading = 0.0;
//        double loudness = 0.0;
//        double hotness = 0.0;
//        double duration = 0.0;
//        String hotTitle = "";
//        String fadingTitle = "";
//
//
//        for (Text val : values) {
//
//            String[] items = val.toString().trim().split("#-#");
//
//            if (items[0].equals("count")) {
//                songCount += Integer.parseInt(items[1]);
//                if (songCount > maxCount) {
//                    maxCount = songCount;
//                    mostSongArtist = key.toString();
//                }
//            }
//            if (items[0].equals("hot")) {
//                String hotInfo = items[1];
//                String[] list = hotInfo.split("\t");
//                hotness = Double.parseDouble(list[1]);
//                hotTitle = list[0].split("%%")[1];
//            }
//            if (items[0].equals("duration")) {
//                String durationInfo = items[1];
//                String[] list = durationInfo.split("\t");
//                duration = Double.parseDouble(list[1]);
//                String title = list[0].split("%%")[1];
//                if (duration > maxLength) {
//                    maxLength = duration;
//                }
//                if (duration < minLength) {
//                    minLength = duration;
//                }
//                medianLength.add(duration);
//                addToList(duration, title);
//
//            }
//            if (items[0].equals("loudest")) {
//                loudCount++;
//                String loudestInfo = items[1];
//                String[] list = loudestInfo.split("\t");
//                avgloudness += ;
//            // Take the average of all loudness
//            avgloudness /= loudCount;
//
//            if (avgloudness > loudnessAvg) {
//                loudnessAvg = avgloudness;
//                loudArtist= key.toString().split("%%")[1];
//
//            }
//
//            if (items[0].equals("fading")){
//                String fadingInfo = items[1];
//                String[] list = fadingInfo.split("\t");
//                fading = Double.parseDouble(list[1]);
//                fadingTitle = list[0].split("%%")[1];
//
//            }
//            if (items[0].equals("dance")){
//                context.write(new Text(items[1]), null);
//
//            }
//            if (items[0].equals("energy")){
//                context.write(new Text(items[1]), null);
//
//            }
//
//
//        }
//        if (hotness > maxHot) {
//            maxHot = hotness;
//            maxHotSong = hotTitle;
//            System.out.println("SecondReducer:: Hot title= "+hotTitle + "  Value= "+hotness);
//        }
//
//        if(fading > maxFading){
//            maxFading = fading;
//            maxFadingSong = fadingTitle;
//            System.out.println("SecondReducer:: Fading title= "+fadingTitle + "  Value= "+fading);
//
//        }
//    }
//
//    @Override
//    protected void cleanup(Context context) throws InterruptedException, IOException {
//
//        // Compute songs count.
//        context.write(new Text("The most songs in dataset: "),
//                new Text(mostSongArtist + "  "+String.valueOf(maxCount)));
//        context.write(new Text("Hottest Song :"),new Text(maxHotSong + " "+maxHot));
//        context.write(new Text(" Artist with the highest total time spent Fading :"),
//                new Text(maxFadingSong + " " + maxFading));
//
//
//
//    }
//}
