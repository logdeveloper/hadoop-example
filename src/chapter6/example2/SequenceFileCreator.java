package chapter6.example2;

import common.AirlinePerformanceParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * The Class .
 */
public class SequenceFileCreator extends Configured implements Tool {

    // org.apache.hadoop.mapred를 사용해서 개발
    // mpareduceJob에서 org.apache.hadoop.mapred.MapFileOutFormat을 출력데이터로 사용할 수 없음.
    // 매퍼를 내부 클래스로 구성
    static class DistanceMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

        private IntWritable outpuekey = new IntWritable();

        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            try {
                AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
                // 운항 거리가 있다면 실행
                if(parser.isDistanceAvailable()){
                    // 칼럼에서 항공 운항 거리를 조회해 출력 데이터의 키로 설정
                    outpuekey.set(parser.getDistance());
                    // 콤마가 들어있던 원래의 라인을 출력함
                    output.collect(outpuekey, value);
                }
            }catch (ArrayIndexOutOfBoundsException ae) {
                // 데이터가 누락되었을 경우, 거리는 0으로 설정
                outpuekey.set(0);
                output.collect(outpuekey, value);
                ae.printStackTrace();
            }catch (Exception e){
                outpuekey.set(0);
                output.collect(outpuekey, value);
                e.printStackTrace();
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {

        JobConf jobConf = new JobConf(SequenceFileCreator.class);

        jobConf.setJobName("SequenceFileCreator");

        jobConf.setMapperClass(DistanceMapper.class);

        // 리듀서가 필요 없으므로  리듀서 잡 크기는 0으로 설정
        jobConf.setNumReduceTasks(0);

        //입출력 경로 설정
        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        // 출력 포맷의 시퀀스 파일 설정
        //출력 포맷을 SequenceFile로 설정
        jobConf.setOutputFormat(SequenceFileOutputFormat.class);

        // 출력키를 항공 운항 거리(IntWritable로 설정)
        jobConf.setOutputKeyClass(IntWritable.class);

        //출력 값을 라인(Text)로 설정
        jobConf.setOutputValueClass(Text.class);

        // 시퀀스 파일의 압축 포맷 설정
        SequenceFileOutputFormat.setCompressOutput(jobConf, true);
        SequenceFileOutputFormat.setOutputCompressorClass(jobConf, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(jobConf, SequenceFile.CompressionType.BLOCK);

        JobClient.runJob(jobConf);

        return 0;
    }

    public static void main(String[] args) throws Exception{

        int res = ToolRunner.run(new Configuration(), new SequenceFileCreator(), args);
        System.out.println("MR-Job Result : " + res);

    }

}
