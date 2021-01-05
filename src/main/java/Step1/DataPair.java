package Step1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * DataPair is a writeable object, to represent a pair of values.
 * Implement the methods for read, write and compare,
 * and meant to be used in the hadoop mapreduce logic
 */
public class DataPair implements Writable {

    private final IntWritable first;
    private final IntWritable second;

    /**
     * Constructor
     * @param first first (as int)
     * @param second second (as int)
     */
    public DataPair(int first, int second) {
        this.first = new IntWritable(first);
        this.second = new IntWritable(second);
    }

    /**
     * Empty constructor
     */
    public DataPair(){
        this.first = new IntWritable(0);
        this.second = new IntWritable(0);
    }

    /**
     * @return first element in pair
     */
    public IntWritable getFirst() {
        return first;
    }

    /**
     * @return second element in pair
     */
    public IntWritable getSecond() {
        return second;
    }

    /**
     * implementation of write method (from writeable)
     * @param dataOutput dataOutput (hadoop.io)
     * @throws IOException if dataOutput doesn't exist
     */
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    /**
     * implementation of write method (from writeable)
     * @param dataInput dataInput (hadoop.io)
     * @throws IOException if dataInput doesn't exist
     */
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    /**
     * Override of toString()
     * Used for producing output files of steps
     * @return string 'first_second' (values)
     */
    @Override
    public String toString() {
        return first.toString() + " " + second.toString();
    }
}
