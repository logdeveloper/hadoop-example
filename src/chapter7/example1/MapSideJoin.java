package chapter7.example1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// 맵 사이드 조인을 실행하는 드라이버 클래스
public class MapSideJoin extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {

        String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

        //입출력 데이터 경로 확인
        if(otherArgs.length != 3){
            System.err.printf("Usage: MapsideJoin <Metadata> <in> <out>");
            System.exit(2);
        }

        // Job 이름 설정
        Job job = new Job(getConf(), "MapsideJoin");

        //잡 객체를 선언하기 전에 Configuration에 분산 캐시 설정
        // DistributedCache의 addCacheFile 메서드를 호출해 로컬 캐시 파일을 추가
        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(),job.getConfiguration());

        //입출력 데이터 경로 설정
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        //Job 클래스 설정
        job.setJarByClass(MapSideJoin.class);
        //Mapper 설정
        job.setMapperClass(MapperWithMapSideJoin.class);
        //맵리듀스 잡은 계산이 필요하지 않으므로 Reducer의 태스크 개수는 0으로 설정
        job.setNumReduceTasks(0);

        // 입출력 데이터 포맷 설정
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 출력키 및 출력값 유형 설정
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        return 0;
    }


    public static void main(String[] args) throws Exception {
        // Tool 인터페이스 실행
        int res = ToolRunner.run(new Configuration(), new MapSideJoin(), args);
        System.out.println("## RESULT :" + res);
    }
}



