package Step_1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Trigram implements WritableComparable<Trigram> {


    private Text word1;
    private Text word2;
    private Text word3;
    private LongWritable r;


    public Trigram(Text word1, Text word2, Text word3, LongWritable r) {
        this.word1 = word1;
        this.word2 = word2;
        this.word3 = word3;
        this.r = r;
    }

    public Text getWord1() {
        return word1;
    }

    public Text getWord2() {
        return word2;
    }

    public Text getWord3() {
        return word3;
    }

    public LongWritable getR() {
        return r;
    }

    public int compareTo(Trigram o) {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {

    }

    public void readFields(DataInput dataInput) throws IOException {

    }
}
