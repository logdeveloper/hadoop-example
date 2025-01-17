package chapter5.example4;

import chapter5.example3.DelayCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ArrivalDelayCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if(args.length != 2){
            System.out.println("Usage : ArrivalDelayCount <input> <output>");
            System.exit(2);
        }

        Job job = new Job(conf, "ArrivalDelayCount");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(ArrivalDelayCount.class);
        job.setMapperClass(ArrivalDelayCounterMapper.class);
        job.setReducerClass(DelayCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);

    }

}
