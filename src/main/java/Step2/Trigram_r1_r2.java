package Step2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Trigram_r1_r2 implements WritableComparable<Trigram_r1_r2> {


    private final Text word1;
    private final Text word2;
    private final Text word3;

    private final IntWritable r0;
    private final IntWritable r1;

    public Trigram_r1_r2(String s, String s1, String s2, int r0, int r1) {
        this.word1 = new Text(s);
        this.word2 = new Text(s1);
        this.word3 = new Text(s2);
        this.r0 = new IntWritable(r0);
        this.r1 = new IntWritable(r1);
    }

    public Trigram_r1_r2(){
        this.word1 = new Text();
        this.word2 = new Text();
        this.word3 = new Text();

        this.r0 = new IntWritable(0);
        this.r1 = new IntWritable(0);
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

    public IntWritable getR0() {
        return r0;
    }

    public IntWritable getR1() {
        return r1;
    }

    @Override
    public int compareTo(Trigram_r1_r2 o) {
        String me = word1.toString() + " " + word2.toString() + " " + word3.toString();
        String other = o.getWord1().toString() + " "  + o.getWord2().toString() + " "  + o.getWord3().toString();
        return me.compareTo(other);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word1.write(dataOutput);
        word2.write(dataOutput);
        word3.write(dataOutput);
        r0.write(dataOutput);
        r1.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word1.readFields(dataInput);
        word2.readFields(dataInput);
        word3.readFields(dataInput);
        r0.readFields(dataInput);
        r1.readFields(dataInput);
    }

    @Override
    public String toString() {
        return word1.toString() + " " + word2.toString() + " " + word3.toString() + " " + r0 + " " + r1 ;
    }
}