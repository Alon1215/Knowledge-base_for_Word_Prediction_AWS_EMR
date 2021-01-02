package Step5;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GramResult implements WritableComparable<GramResult> {



    private final Text word1;
    private final Text word2;
    private final Text word3;
    private final DoubleWritable prob;

    public GramResult(String s, String s1, String s2, double prob) {
        this.word1 = new Text(s);
        this.word2 = new Text(s1);
        this.word3 = new Text(s2);
        this.prob = new DoubleWritable(prob);
    }

    public GramResult(){
        this.word1 = new Text();
        this.word2 = new Text();
        this.word3 = new Text();
        this.prob = new DoubleWritable(0);
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

    public DoubleWritable getProb() {
        return prob;
    }

    @Override
    public int compareTo(GramResult o) {
        String me = word1.toString() + " " + word2.toString();
        String other = o.getWord1().toString() + " "  + o.getWord2().toString();
        int lexicalCompare = me.compareTo(other);
        if (lexicalCompare == 0){
            return prob.compareTo(o.prob);
        }
        return lexicalCompare;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word1.write(dataOutput);
        word2.write(dataOutput);
        word3.write(dataOutput);
        prob.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word1.readFields(dataInput);
        word2.readFields(dataInput);
        word3.readFields(dataInput);
        prob.readFields(dataInput);
    }

    @Override
    public String toString() {
        return word1.toString() + " " + word2.toString() + " " + word3.toString();
    }
}
