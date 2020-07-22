package chapter6.example2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The Class .
 */
public class MapFileCreator extends Configured implements Tool {


    // 맵파일은 키 값을 검색할 수 있게 색인과 함께 정렬된 시퀀스 파일
    // 맵파일은 물리적으로 색인이 저장된  index 파일과 데이터 내용이 저장돼 있는 data 파일로 구성됨.
    // 이미 HDFS에 저장돼 있는 시퀀스 파일을 변화해 맵파일로 생성할 수 있다.

    public static void main(String[] args) throws Exception{

        int res = ToolRunner.run(new Configuration(), new MapFileCreator(), args);
        System.out.println("MR-Job Result : "+ res);

    }


    @Override
    public int run(String[] args) throws Exception {

        JobConf jobConf = new JobConf(MapFileCreator.class);

        jobConf.setJobName("MapFileCreator");

        // 이 드라이버 클래스는 데이터를 분석할 필요가 없기 때문에 별도의 매퍼와 리듀서 클래스를 설정하지 않음.
        // JobClient는 매퍼와 리듀서 클래스를 설정하지 않을 경우 org.hadoop.mapred.Reducer를 기본값으로 설정

        //입출력 경로 설정
        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));


        // 입력 데이터를 시퀀스 파일로 설정
        jobConf.setInputFormat(SequenceFileInputFormat.class);

        // 출력데이터를 맵파일로 설정
        jobConf.setOutputFormat(MapFileOutputFormat.class);

        // 출력 데이터의 키를 항공 운항 거리(IntWritable)로 설정
        jobConf.setOutputKeyClass(IntWritable.class);

        // 시퀀스파일 압축 포맷 설정
        SequenceFileOutputFormat.setCompressOutput(jobConf, true);
        SequenceFileOutputFormat.setOutputCompressorClass(jobConf, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(jobConf, SequenceFile.CompressionType.BLOCK);

        JobClient.runJob(jobConf);

        return 0;
    }

}
