package Step1;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Trigram implements WritableComparable<Trigram> {


    private final Text word1;
    private final Text word2;
    private final Text word3;

    public Trigram(String s, String s1, String s2) {
        this.word1 = new Text(s);
        this.word2 = new Text(s1);
        this.word3 = new Text(s2);
    }

    public Trigram(){
        this.word1 = new Text("");
        this.word2 = new Text("");
        this.word3 = new Text("");
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

    public int compareTo(Trigram o) {
            String me = word1.toString() + " " + word2.toString() + " " + word3.toString();
            String other = o.getWord1().toString() + " "  + o.getWord2().toString() + " "  + o.getWord3().toString();
            return me.compareTo(other);
    }

    public void write(DataOutput dataOutput) throws IOException {
        word1.write(dataOutput);
        word2.write(dataOutput);
        word3.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        word1.readFields(dataInput);
        word2.readFields(dataInput);
        word3.readFields(dataInput);
    }

    @Override
    public String toString() {
        return word1.toString() + " " + word2.toString() + " " + word3.toString();
    }
}
