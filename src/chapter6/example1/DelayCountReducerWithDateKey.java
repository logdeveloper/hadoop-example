package chapter6.example1;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * The Class .
 */
public class DelayCountReducerWithDateKey extends Reducer<DateKey, IntWritable, DateKey, IntWritable> {

    // 그룹핑 파티셔너와 그룹핑 비교기에 의해 같은 연도로 그룹핑돼 있는 데이터가 전달됨

    private MultipleOutputs<DateKey,IntWritable> mos;

    private DateKey outputkey = new DateKey();

    private IntWritable result = new IntWritable();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<DateKey,IntWritable>(context);
    }


    public void reduce(DateKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        String[] columns = key.getYear().split(",");

        int sum = 0;

        // month value backup
        Integer bMonth = key.getMonth();

        if(columns[0].equals("D")){

            for (IntWritable value : values){
                // 순회하면서 백업된 월과 현재 월이 일치하지 않을 때는 리듀서의 출력 데이터에 백업된 월의 지연 횟수를 출력
                if(bMonth != key.getMonth()){
                    result.set(sum);
                    outputkey.setYear(key.getYear().substring(2));
                    outputkey.setMonth(bMonth);

                    mos.write("departure", outputkey, result);
                    // 다음 순서는 0으로 초기화
                    sum= 0;
                }

                sum += value.get();
                bMonth = key.getMonth();
            }
            if(key.getMonth() == bMonth){
                // Iterable 객체 순회 후, 합산한 월의 지연 횟수 출력
                outputkey.setYear(key.getYear().substring(2));
                outputkey.setMonth(key.getMonth());
                result.set(sum);
                mos.write("departure", outputkey, result);
            }
        }else {
            for (IntWritable value : values){
                if(bMonth != key.getMonth()){
                    result.set(sum);
                    outputkey.setYear(key.getYear().substring(2));
                    outputkey.setMonth(bMonth);

                    mos.write("arrival", outputkey, result);
                    sum= 0;
                }
                sum += value.get();
                bMonth = key.getMonth();

            }
            if(key.getMonth() == bMonth){
                outputkey.setYear(key.getYear().substring(2));
                outputkey.setMonth(key.getMonth());
                result.set(sum);
                mos.write("arrival", outputkey, result);
            }
        }

    }
    @Override
    public void cleanup(Context context) throws  IOException, InterruptedException {
            mos.close();
    }

}
