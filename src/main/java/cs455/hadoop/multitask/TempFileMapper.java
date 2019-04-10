package cs455.hadoop.multitask;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TempFileMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split("\t");
//        System.out.println("TempFileMapper:: "+value.toString());
        String[] outList = val[0].split("#-#");
        String outKey;
        String outVal;
        if (outList[0].equals("loudest")) {
            outKey = "loudest#-#" + outList[1];
            outVal = val[1];
        }else{
            outKey = outList[0];
            outVal = outList[1] + "%-%" + val[1];
        }
        context.write(new Text(outKey), new Text(outVal));
    }
}