package chapter6.example1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupKeyPartitioner extends Partitioner {

    @Override
    public int getPartition(Object o, Object o2, int numpartition) {
        DateKey key = (DateKey) o;
        IntWritable val = (IntWritable) o2;

        int hash = key.getYear().hashCode();
        int partition =hash% numPartition;
        return partition;
    }
}
