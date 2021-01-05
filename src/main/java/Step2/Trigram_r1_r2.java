package Step2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 3Gram implementation & representation for the hadoop flow, and assignment logic.
 * Class implements WritableComparable, with the required methods.
 * Trigram is used through the 5 steps, in order to represent a 3gram which is:
 *      Writeable
 *      Comparable
 *      String represented as the given 3gram + his r values
 */
public class Trigram_r1_r2 implements WritableComparable<Trigram_r1_r2> {


    private final Text word1;
    private final Text word2;
    private final Text word3;

    private final IntWritable r0;
    private final IntWritable r1;

    /**
     * Constructor
     * @param s first word
     * @param s1 second word
     * @param s2 third word
     * @param r0 Gram's occurrences in Corpus 0
     * @param r1 Gram's occurrences in Corpus 1
     */
    public Trigram_r1_r2(String s, String s1, String s2, int r0, int r1) {
        this.word1 = new Text(s);
        this.word2 = new Text(s1);
        this.word3 = new Text(s2);
        this.r0 = new IntWritable(r0);
        this.r1 = new IntWritable(r1);
    }

    /**
     * Empty constructor
     */
    public Trigram_r1_r2(){
        this.word1 = new Text();
        this.word2 = new Text();
        this.word3 = new Text();

        this.r0 = new IntWritable(0);
        this.r1 = new IntWritable(0);
    }



    /**
     * @return Word 1
     */
    public Text getWord1() {
        return word1;
    }

    /**
     * @return Word 2
     */
    public Text getWord2() {
        return word2;
    }

    /**
     * @return Word 3
     */
    public Text getWord3() {
        return word3;
    }

    /**
     * @return Word r_0
     */
    public IntWritable getR0() {
        return r0;
    }

    /**
     * @return Word r_1
     */
    public IntWritable getR1() {
        return r1;
    }

    /**
     * Compare to other 3grams
     * @param o other 3gram
     * @return int value, result of comparing the 2 3grams
     */
    @Override
    public int compareTo(Trigram_r1_r2 o) {
        String me = word1.toString() + " " + word2.toString() + " " + word3.toString();
        String other = o.getWord1().toString() + " "  + o.getWord2().toString() + " "  + o.getWord3().toString();
        return me.compareTo(other);
    }

    /**
     * implementation of write method (from writeable)
     * @param dataOutput dataOutput (hadoop.io)
     * @throws IOException if dataOutput doesn't exist
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word1.write(dataOutput);
        word2.write(dataOutput);
        word3.write(dataOutput);
        r0.write(dataOutput);
        r1.write(dataOutput);
    }

    /**
     * implementation of write method (from writeable)
     * @param dataInput dataInput (hadoop.io)
     * @throws IOException if dataInput doesn't exist
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word1.readFields(dataInput);
        word2.readFields(dataInput);
        word3.readFields(dataInput);
        r0.readFields(dataInput);
        r1.readFields(dataInput);
    }

    /**
     * Override of toString()
     * Used for producing output files of steps
     * @return string 's1 s2 s3 r0 r1' (string values)
     */
    @Override
    public String toString() {
        return word1.toString() + " " + word2.toString() + " " + word3.toString() + " " + r0 + " " + r1 ;
    }
}