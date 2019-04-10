package cs455.hadoop.utils;


import java.util.Comparator;

public class DoubleComparator implements Comparator<String> {
    /**
     * This is a custom comparator for PriorityQueue to store string with format "SongTitle%%energy".
     * Energy or danceability is double.
     * So we store it in Priority queue (which behaves as min Heap map to store items based on the double value after %%
     * @param str1: String received from the mapper in format "Title%%double"
     * @param str2: same as str1
     *
     */
    @Override
    public int compare(String str1, String str2){
        String[] items1 = str1.split("%-%");
        String[] items2 = str2.split("%-%");
        if (Double.parseDouble(items1[1]) > Double.parseDouble(items2[1])){
            return 1;
        }else if (Double.parseDouble(items1[1]) < Double.parseDouble(items2[1])){
            return -1;
        }else{
            return 0;
        }
    }


}
