package chapter5.example3;

import common.AirlinePerformanceParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The Class .
 */
public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable outputValue = new IntWritable(1);

    private Text outputkey = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

        outputkey.set(parser.getYear()+","+ parser.getMonth());
        if(parser.getDepartureDelayTime() > 0){
            context.write(outputkey, outputValue);
        }

    }

}
