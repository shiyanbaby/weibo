package weibo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgCount implements Writable {
   private float count;
   private float avg;

    public AvgCount() {
    }

    public AvgCount(float count, float avg) {
        this.count = count;
        this.avg = avg;
    }

    public float getCount() {
        return count;
    }

    public void setCount(float count) {
        this.count = count;
    }

    public float getAvg() {
        return avg;
    }

    public void setAvg(float avg) {
        this.avg = avg;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(count);
        dataOutput.writeFloat(avg);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
             count=dataInput.readFloat();
             avg=dataInput.readFloat();
    }

    @Override
    public String toString() {
        return "AvgCount{" +
                "count=" + count +
                ", avg=" + avg +
                '}';
    }
}
