package Step3;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedKey implements WritableComparable<TaggedKey> {
    private IntWritable tag;
    private Text key;

    public TaggedKey(int tag, Text key) {
        this.tag = new IntWritable(tag);
        this.key = key;
    }

    public TaggedKey() {
        key = new Text();
        tag = new IntWritable(0);
    }


    public IntWritable getTag(){
        return tag;
    }
    public Text getKey(){
        return key;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof TaggedKey) && (this.tag == ((TaggedKey) o).tag) &&(this.key.equals(((TaggedKey) o).key));
    }

    @Override
    public int compareTo(TaggedKey other) {
        int i = key.toString().compareTo(other.key.toString());
        if (i==0)                                              
            i=tag.compareTo(other.tag);
        return i;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        tag.write(dataOutput);
        key.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tag.readFields(dataInput);
        key.readFields(dataInput);
    }
}