package chapter6.example1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateKey implements WritableComparable<DateKey> {

    private String year;
    private Integer month;

    public DateKey() {
    }

    public DateKey(String year, Integer month) {
        this.year = year;
        this.month = month;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    @Override
    public String toString() {
        return "DateKey{" +
                "year='" + year + '\'' +
                ", month=" + month +
                '}';
    }


    @Override
    public int compareTo(DateKey key) {
        // 복합티와 복합키를 비교해 순서를 정할때 사용
        int result = year.compareTo(key.year);
        if (0 == result) {
            result = month.compareTo(key.month);
        }
        return 0;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 출력 스트림에서 연도와 월을 출력
        // 이때 스트림에서 데이터를 읽고, 출력하는 작업에는 하둡에서 제공하는 WritableUtils 이용
        WritableUtils.writeString(dataOutput, year);
        dataOutput.writeInt(month);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        // 입력 스트림에서 연도와 월을 조회
        // 이때 스트림에서 데이터를 읽고, 출력하는 작업에는 하둡에서 제공하는 WritableUtils 이용
        year = WritableUtils.readString(in);
        month = in.readInt();
    }
}
