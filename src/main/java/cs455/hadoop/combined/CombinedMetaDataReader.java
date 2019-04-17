package cs455.hadoop.combined;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


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
public class CombinedMetaDataReader extends Mapper<LongWritable, Text, Text, Text> {
    private int songIdx = 8;
    private int familiarityIdx = 1;
    private int hotIdx = 2;
    private int yearIdx = 14;
    private int longitudeIdx = 5;


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the header from the csv

        if (key.get() == 0l && value.toString().contains("song_id")) {
            ArrayList<String> header = new ArrayList(Arrays.asList(value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0)));
            familiarityIdx = header.indexOf("artist_familiarity");
            songIdx = header.indexOf("song_id");
            hotIdx = header.indexOf("artist_hotttnesss");
            yearIdx = header.indexOf("year");

        } else {
            String[] items = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 0);
            try {
                String songId = items[songIdx].trim();
                double artistHot = Double.parseDouble(items[hotIdx].trim());
                int year = Integer.parseInt(items[yearIdx].trim());
                String longitude = items[longitudeIdx].trim();
                double artistfamiliar = Double.parseDouble(items[familiarityIdx].trim());
                if (year > 1000 && artistHot > 0.0 && artistfamiliar > 0.0)
                    context.write(new Text(songId), new Text("meta#-#" + artistfamiliar + "," + artistHot + "," +
                            year+","+longitude));
            }catch (Exception e){
                System.err.println("Error reading metadata for data prepration.");
            }

        }
    }
}