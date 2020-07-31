package common;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CarrierCodeParser extends Mapper<LongWritable, Text, Text, Text> {

    public final static String DATA_TAG = "A";

    private Text outputkey = new Text();
    private Text outputValue = new Text();

    public CarrierCodeParser(String line) {
    }

    public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

        if(key.get() > 0 ){
            String[] colums = value.toString().replaceAll("\"","").split(",");
            if(colums != null && colums.length>0){
                outputkey.set(colums[0] + "_" + DATA_TAG);
                outputValue.set(colums[1]);
                context.write(outputkey, outputValue);
            }
        }

    }


}
