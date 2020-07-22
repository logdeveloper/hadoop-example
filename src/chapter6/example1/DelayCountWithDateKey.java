package chapter6.example1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The Class .
 */
public class DelayCountWithDateKey extends Configured implements Tool {


    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new DelayCountWithDateKey(), args);
        System.out.println("MR-Job Result: " + res);

    }

    @Override
    public int run(String[] args) throws Exception {

        String[] othersArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

        if(othersArgs.length !=2){
            System.err.printf("Usage : DelaycountWithDateKey <in> <output>");
            System.exit(2);
        }

        Job job = new Job(getConf(), "DelayCountWithDateKey");

        FileInputFormat.addInputPath(job, new Path(othersArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(othersArgs[1]));

        //잡클래스 생성
        job.setJarByClass(DelayCountWithDateKey.class);

        // 그룹키 파티셔너, 그룹키 비교기, 복합키 비교기 등록
        job.setPartitionerClass(GroupKeyPartitioner.class);
        job.setGroupingComparatorClass(GroupKeyComparator.class);
        job.setSortComparatorClass(DateKeyComparator.class);

        // 매퍼 클래스 생성
        job.setMapperClass(DelayCountMapperWithDateKey.class);
        // 리듀서 클래스 생성
        job.setReducerClass(DelayCountReducerWithDateKey.class);

        //매퍼의 출력 데이터 포맷에 다음과 같이 복합키와  지연횟수(IntWritable)를 설정
        job.setMapOutputKeyClass(DateKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        //입출력 데이터 포맷 설정
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //출력키 및 출력값 유형
        //리듀서의 출력 데이터 포맷에 다음과 같이 복합키와  지연횟수(IntWritable)를 설정
        job.setOutputKeyClass(DateKey.class);
        job.setOutputValueClass(IntWritable.class);

        // MultipleOutputs 설정

        MultipleOutputs.addNamedOutput(job, "departure",
                TextOutputFormat.class, DateKey.class, IntWritable.class);

        MultipleOutputs.addNamedOutput(job, "arrival",
                TextOutputFormat.class, DateKey.class, IntWritable.class);

        job.waitForCompletion(true);
        return 0;
    }
}
