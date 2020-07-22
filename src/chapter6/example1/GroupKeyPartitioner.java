package chapter6.example1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupKeyPartitioner extends Partitioner {

    //파티셔너는 메서드를 호출하여 파티셔닝 번호를 조회한다.
    @Override
    public int getPartition(Object o, Object o2, int numpartition) {
        // 연도에 대한 해시코드를 조회해 파티션 번호를 생성한다.
        DateKey key = (DateKey) o;
        IntWritable val = (IntWritable) o2;

        int hash = key.getYear().hashCode();
        int partition =hash% numpartition;
        return partition;
    }
}
