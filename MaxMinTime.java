package weibo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class MaxMinTime implements Writable{
  private   Date max;
  private Date min;
  private  long count;

    public MaxMinTime() {

    }

    public MaxMinTime(Date max, Date min, long count) {
        this.max = max;
        this.min = min;
        this.count = count;
    }

    public Date getMax() {
        return max;
    }

    public void setMax(Date max) {
        this.max = max;
    }

    public Date getMin() {
        return min;
    }

    public void setMin(Date min) {
        this.min = min;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
       dataOutput.writeLong(max.getTime());
       dataOutput.writeLong(min.getTime());
       dataOutput.writeLong(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
           max=new Date(dataInput.readLong());
           min=new Date(dataInput.readLong());
           count=dataInput.readLong();
    }

    @Override
    public String toString() {
        return "MaxMinTime{" +
                "max=" + max +
                ", min=" + min +
                ", count=" + count +
                '}';
    }
}
