package chapter7.example1;


import common.AirlinePerformanceParser;
import common.CarrierCodeParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

public class MapperWithMapSideJoin extends Mapper<LongWritable, Text, Text, Text> {

    // 분산 캐시에서 조회한 항공사 코드 데이터를 저장할 HashMap 객체를 선언함.
    // 맵 매서드가 실행될 때마다 분산 캐시를 조회할 수도 있지만, 캐시파일에 불필요한 I/O를 발생시킴.
    private Hashtable<String, String> joinMap = new Hashtable<String, String>();

    private Text outputKey = new Text();

    // 매퍼가 분산 캐시를 불필요하게 접근하는 것을 줄이도록 setup 매서드에서만 분산 캐시를 조회함.
    @Override
    public void setup(Context context) throws IOException, InterruptedException{

        try {
            // 분산 캐쉬에 있는 전체 로컬 파일을 조회함.
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(cacheFiles != null && cacheFiles.length > 0){ //캐시 파일이 정상적으로 반환되면
                String line;
                // 전체 파일에서 첫번째 파일을 이용해 BufferedReader를 생성
                // 파일 배열에서 첫 번째 파일만 사용하는 이유는 우리가 하나의 텍스트 파일만 캐시에 등록하기 때문.
                BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                try{
                    //이제 순회하면서 CarrierCodeParser를 이용해 항공사 코드와 항공사 이름을 조회
                    while ((line= br.readLine()) != null){
                        CarrierCodeParser codeParser = new CarrierCodeParser(line);
                        //조회한 정보를 전역변수로 선언한 HashMap 객체에 등록
                        joinMap.put(codeParser.getCarrierCode(), codeParser.getCarrierName());
                    }
                }finally {
                    br.close();
                }
            }else {
                System.out.println("cache files is null!");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        // HashMap 객체에서 해당 항공사 이름을 조회해 데이터를 출력
        AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
        outputKey.set(parser.getUniqueCarrier());
        context.write(outputKey, new Text(joinMap.get(parser.getUniqueCarrier()) + "\t" + value.toString()));
    }

}
