package chapter53;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * The Class .
 */
public class DepartureDelayCount {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        if(args.length != 2){
            System.err.println("Usage: DepartureDelayCount <Input> <Output>");
            System.exit(2);
        }

        Job job = new Job(conf, "DepartureDelayCount");
//        JobConf job = new JobConf(conf);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(DepartureDelayCount.class);
        job.setMapperClass(DepartureDelayCountMapper.class);
        job.setReducerClass(DelayCountReducer.class);

    }
}

