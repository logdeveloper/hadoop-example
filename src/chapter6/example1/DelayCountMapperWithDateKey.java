package chapter6.example1;

import common.AirlinePerformanceParser;
import common.DelayCounters;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The Class .
 */
public class DelayCountMapperWithDateKey extends Mapper<LongWritable, Text, DateKey, IntWritable> {

    //맵 출력값
    private final static IntWritable outputValue = new IntWritable(1);

    // 맵 출력키
    private DateKey outputKey = new DateKey();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

        // 출발 지연 데이터 출력
        if(parser.isDepartureDelayAvailable()){
            if(parser.getDepartureDelayTime() > 0){
                // 정상 출발시, 출력키 설정
                // 출발과 도착을 동시에 처리하므로 D와 A로 구분
                outputKey.setYear("D " + parser.getYear());
                outputKey.setMonth(parser.getMonth());

                // 출력데이터 생성
                context.write(outputKey, outputValue);
            }else if(parser.getDepartureDelayTime() == 0){
                // 정시 도착
                context.getCounter(DelayCounters.scheduled_departure).increment(1);
            }else if(parser.getDepartureDelayTime() <0){
                // 출발 시간보다 일찍 출발
                context.getCounter(DelayCounters.early_departure).increment(1);
            }
        }else {
            // 출발할 수 없을 경우
            context.getCounter(DelayCounters.not_available_departure).increment(1);
        }

        //도착 지연 데이터 출력
        if(parser.isArriveDelayAvailable()){
            if(parser.getArriveDelayTime() > 0){
                // 정상 도착시, 출력키 설정
                outputKey.setYear("A " + parser.getYear());
                outputKey.setMonth(parser.getMonth());

                // 출력데이터 생성
                context.write(outputKey, outputValue);
            }else if(parser.getArriveDelayTime() == 0){
                context.getCounter(DelayCounters.scheduled_arrival).increment(1);
            }else if(parser.getArriveDelayTime() < 0){
                context.getCounter(DelayCounters.early_arrival).increment(1);
            }
        }else {
            context.getCounter(DelayCounters.not_available_arrival).increment(1);
        }

    }

}
