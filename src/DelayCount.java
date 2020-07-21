import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * The Class .
 */
public class DelayCount {

    public int run(String[] args){
//        String[] otherArgs = new GenericOptionsParser(
//                getConf(),args).getRemainingArgs();
//
//        if(otherArgs.length != 2){
//            System.err.println("Usage_log : DelayCount <in> <out>");
//            System.exit(2);
//        }

//        Job job = new Job(getConf(), "DelayCount");
//
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

//        job.setJarByClass(DelayCount.class);
//        job.setMapperClass(DelayCountMapper.class);
//        job.setReducerClass(DelayCountReducer.class);


    return 1;


    }


    public static void main(String[] args) {

    }



}
