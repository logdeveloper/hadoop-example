package chapter6.example2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The Class .
 */
public class SearchValueList extends Configured implements Tool {


    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new SearchValueList(), args);
        System.out.println("MR-Job Result: " + res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path path = new Path(args[0]);
        // 하둡에서  제공하는 파일시스템을 추상화한 클래스, 로컬 파일 시스템이나 HDFS나 어떤 파일 시스템을 이용하든 FileSystme을 거쳐야한다.
        FileSystem fs = path.getFileSystem(getConf());

        // 맵파일 조회 , MapFile.Reader는 맵파일에 저장된 데이터 목록을 순회하면서 데이터를 조회하는 API
        MapFile.Reader[] readers = MapFileOutputFormat.getReaders(fs, path, getConf());

        //검색할 키를 지정할 객체를 선언
        // 맵파일 키가 운항거리기 때문에 IntWritable형으로 설정
        IntWritable key = new IntWritable();
        key.set(Integer.parseInt(args[2]));

        //검색 값을 저장할 객체를 선언
        Text value = new Text();

        // 맵파일에 접근하려면 파티션 정보가 필요하므로 해시 파티셔너를 생성.
        //파티셔너를 이용해 검색 키가 저장된 맵파일 조회
        Partitioner<IntWritable, Text> partitioner = new HashPartitioner<>();
        // getPartition메서드는 특정 키에 대한 파티션 번호를 반환.
        // 앞서 생성한 Reader 배열에서 이 파시녕 번호에 해당하는 Reader를 조회함.
        MapFile.Reader reader = readers[partitioner.getPartition(key, value, readers.length)];

        //검색 결과 확인
        // Reader의 get 메서드를 사용해 특정 키에 해당하는 값을 검색.
        // 반환되는 값은 데이터 목록의 첫번째 값.
        Writable entry = reader.get(key, value);
        if(entry == null){
            System.out.println("The requested key was not found");
        }

        //맵파일을 순회하며 키와 값을 출력
        IntWritable nextkey = new IntWritable();

        do{
            System.out.println(value.toString());
            // next 메소드는 다음 순서의 데이터로 위치를 이동하고, key와 value 파라미터에 현재 위치의 키와 값을 설정.
        }while (reader.next(nextkey, value) && key.equals(nextkey));

        return 0;
    }


}
