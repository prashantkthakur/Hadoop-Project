package cs455.hadoop.avgtask;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.stream.Collectors;

public class AvgReducer  extends Reducer<Text, Text, Text, Text> {
    private double startTime;

    private int songKey, timeSig, mode;
    private double tempo, dance, energy, duration, loudness, stopFade, startFade;


    private long counter;
    private ArrayList<Double> resultPitch = new ArrayList<>();
    private ArrayList<Double> resultTimber = new ArrayList<>();
    private ArrayList<Double> resultMaxLoudness = new ArrayList<>();
    private ArrayList<Double> resultMaxLoudnessTime = new ArrayList<>();
    private ArrayList<Double> resultStartLoudness = new ArrayList<>();

    private void addValues(ArrayList<Double>result, String[] list){
        for (int i=0; i < list.length; ++i){
            try{
                result.set(i, result.get(i) + Double.parseDouble(list[i]));
            }catch (IndexOutOfBoundsException aib){
                result.add(Double.parseDouble(list[i]));
            }
        }
    }


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Key is "avg" so all data are collected into values.
        // One mapper sends sum of corresponding values and the counter.
        String[] pitch;
        String[] timber;
        String[] maxLoudness;
        String[] maxLoudnessTime;
        String[] startLoudness;


        if (key.toString().equals("avg-fake-hot")) {
            for (Text val : values) {
                String[] items = val.toString().split(",");
                //tempo + ","+timeSig+","+dance + "," +duration+","+ mode +","+
                // energy + "," + songKey + "," +loudness+","+stopFade+","+startFadeIdx
                tempo = Math.max(tempo, Double.parseDouble(items[0]));
                timeSig = Math.max(timeSig, Integer.parseInt(items[1]));
                dance = Math.max(dance, Double.parseDouble(items[2]));
                duration = Math.max(duration, Double.parseDouble(items[3]));
                mode = Math.max(mode, Integer.parseInt(items[4]));
                energy = Math.max(energy, Double.parseDouble(items[5]));
                songKey = Math.max(songKey, Integer.parseInt(items[6]));
                loudness = Math.max(loudness, Double.parseDouble(items[7]));
                stopFade = Math.min(stopFade, Double.parseDouble(items[8]));
                startFade = Math.max(startFade, Double.parseDouble(items[9]));
            }

        } else if (key.toString().equals("avg")) {
            for (Text val : values) {
                String[] items = val.toString().split("#-#");
                switch (items[0]) {
                    case "start-time":
                        startTime += Double.parseDouble(items[1]);
                        break;
                    case "counter":
                        counter = Long.parseLong(items[1]);
                        break;
                    case "pitch":
                        pitch = items[1].split("\\s+");
                        addValues(resultPitch, pitch);
                        break;
                    case "timber":
                        timber = items[1].split("\\s+");
                        addValues(resultTimber, timber);
                        break;
                    case "max-loud":
                        maxLoudness = items[1].split("\\s+");
                        addValues(resultMaxLoudness, maxLoudness);
                        break;

                    case "loud-time":
                        maxLoudnessTime = items[1].split("\\s+");
                        addValues(resultMaxLoudnessTime, maxLoudnessTime);
                        break;

                    case "start-loud":
                        startLoudness = items[1].split("\\s+");
                        addValues(resultStartLoudness, startLoudness);
                        break;

                }
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Average Song: "), new Text("\n--------------------------\n"));
        context.write(new Text("Start Time = "), new Text(String.valueOf(startTime/counter)));
        context.write(new Text("Total Songs = "), new Text(String.valueOf(counter)));

        String tmp = resultPitch.stream().map(v -> v/counter).map(Object::toString)
                .collect(Collectors.joining(" "));
        context.write(new Text("Pitch = "), new Text(tmp));
        tmp = resultPitch.stream().map(Object::toString)
                .collect(Collectors.joining(" "));
        context.write(new Text("Original Pitch = "), new Text(tmp));
        tmp = resultTimber.stream().map(v -> v/counter).map(Object::toString)
                .collect(Collectors.joining(" "));
        context.write(new Text("Timber = "), new Text(tmp));

        tmp = resultMaxLoudness.stream().map(v -> v/counter).map(Object::toString)
                .collect(Collectors.joining(" "));
        context.write( new Text("Max Loudness = "), new Text(tmp));

        tmp = resultMaxLoudnessTime.stream().map(v -> v/counter).map(Object::toString)
                .collect(Collectors.joining(" "));
        context.write(new Text("Max Loudness Time = "), new Text(tmp));

        tmp = resultStartLoudness.stream().map(v -> v/counter).map(Object::toString)
                .collect(Collectors.joining(" "));
        context.write(new Text("Start Loudness = "), new Text(tmp));

        // Write out fake-hot
        String alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"; //9
        String songId = "S";
        Random r = new Random();
        for (int i=0; i<17; i++)
            songId += alphabet.charAt(r.nextInt(alphabet.length()));
        String artistName = "Prashant Thakur";
        context.write(new Text("\nFake Hot: "+songId +","+artistName),
                new Text(tempo + ","+timeSig+","+dance + "," +duration+","+ mode +","+
                                energy + "," + songKey + "," +loudness+","+stopFade+","+startFade));

    }
}
