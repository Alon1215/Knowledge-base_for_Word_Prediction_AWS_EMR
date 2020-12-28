package Step2;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class N_r implements WritableComparable<N_r> {
    private final IntWritable n;
    private final IntWritable r;
    private final ArrayWritable array;

    public N_r(IntWritable n, IntWritable r, ArrayWritable array) {
        this.n = n;
        this.r = r;
        this.array = array;
    }

    @Override
    public int compareTo(N_r o) {
        int nComparison = n.compareTo(o.n);
        if (nComparison == 0) return r.compareTo(o.r);
        return nComparison;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        n.write(dataOutput);
        r.write(dataOutput);
        array.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        n.readFields(dataInput);
        r.readFields(dataInput);
        array.readFIelds(dataOutput);
    }


}
