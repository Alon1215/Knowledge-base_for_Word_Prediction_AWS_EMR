package Step1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import javax.xml.crypto.Data;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataPair implements Writable {

    private final IntWritable first;
    private final IntWritable second;


    public DataPair(int first, int second) {
        this.first = new IntWritable(first);
        this.second = new IntWritable(second);
    }
    public DataPair(){
        this.first = new IntWritable(0);
        this.second = new IntWritable(0);
    }
    public IntWritable getFirst() {
        return first;
    }

    public IntWritable getSecond() {
        return second;
    }

    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public String toString() {
        return first.toString() + " " + second.toString();
    }
}
