package Step3;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * TaggedKey implementation,
 * in order to Perform ReduceSideJoin using a secondary sort on the tagged keys.
 */
public class TaggedKey implements WritableComparable<TaggedKey> {
    private final IntWritable tag;
    private final Text key;


    /**
     * Contructor, receives tag & key
     * @param tag -1 / 1, indicating if the value is 3gram or  <N_0_r, T_0_r> (same for Corpus 1)
     * @param key N_0_r (or 1), r value as defined.
     */
    public TaggedKey(int tag, Text key) {
        this.tag = new IntWritable(tag);
        this.key = key;
    }

    /**
     * Empty constructor
     */
    public TaggedKey() {
        key = new Text();
        tag = new IntWritable(0);
    }

    /**
     * @return tag
     */
    public IntWritable getTag(){
        return tag;
    }


    /**
     * @return key
     */
    public Text getKey(){
        return key;
    }


    @Override
    public boolean equals(Object o) {
        return (o instanceof TaggedKey) && (this.tag == ((TaggedKey) o).tag) &&(this.key.equals(((TaggedKey) o).key));
    }

    /**
     * Compare between to other TaggedKey.
     * compareTo will be used to sort <N_0_r, T_0_r> before the 3grams,
     * and to implement the logic of the secondary sort.
     * @param other TaggedKey
     * @return int value, result of comparing the 2 3grams
     */
    @Override
    public int compareTo(TaggedKey other) {
        int i = key.toString().compareTo(other.key.toString());
        if (i==0)                                              
            i=tag.compareTo(other.tag);
        return i;
    }

    /**
     * implementation of write method (from writeable)
     * @param dataOutput dataOutput (hadoop.io)
     * @throws IOException if dataOutput doesn't exist
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        tag.write(dataOutput);
        key.write(dataOutput);
    }

    /**
     * implementation of write method (from writeable)
     * @param dataInput dataInput (hadoop.io)
     * @throws IOException if dataInput doesn't exist
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tag.readFields(dataInput);
        key.readFields(dataInput);
    }
}