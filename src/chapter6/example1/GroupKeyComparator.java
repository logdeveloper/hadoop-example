package chapter6.example1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * The Class .
 */
public class GroupKeyComparator extends WritableComparator {

    protected GroupKeyComparator() {
        super(DateKey.class, true);
    }

    public int compare(WritableComparable w1, WritableComparable w2){
        // 복합키의 연도 비교
        DateKey k1 = (DateKey) w1;
        DateKey k2 = (DateKey) w2;
        return k1.getYear().compareTo(k2.getYear());
    }
}
