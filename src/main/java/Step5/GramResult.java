package Step5;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 3Gram implementation & representation for the hadoop flow, and assignment logic.
 * It's mainly difference from the other representation of 3gram, by the way compareTo() is implemented.
 * Class is used to produce on the final step the right sort in the 3grams.
 * Class implements WritableComparable, with the required methods.
 * Trigram is used int Step 5, in order to represent a 3gram which is:
 *      Writeable
 *      Comparable
 *      String represented as the given 3gram + his r values
 */
public class GramResult implements WritableComparable<GramResult> {

    private final Text word1;
    private final Text word2;
    private final Text word3;
    private final DoubleWritable prob;

    /**
     * Constructor
     * @param s first word
     * @param s1 second word
     * @param s2 third word
     * @param prob = probability as defined in the assignment
     */
    public GramResult(String s, String s1, String s2, double prob) {
        this.word1 = new Text(s);
        this.word2 = new Text(s1);
        this.word3 = new Text(s2);
        this.prob = new DoubleWritable(prob);
    }

    /**
     * Empty constructor
     */
    public GramResult(){
        this.word1 = new Text();
        this.word2 = new Text();
        this.word3 = new Text();
        this.prob = new DoubleWritable(0);
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
     * @return probability
     */
    public DoubleWritable getProb() {
        return prob;
    }

    /**
     * Compare to other 3grams, as needed for the final output.
     * @param o other 3gram
     * @return int value, result of comparing the 2 3grams
     */
    @Override
    public int compareTo(GramResult o) {
        String me = word1.toString() + " " + word2.toString();
        String other = o.getWord1().toString() + " "  + o.getWord2().toString();
        int lexicalCompare = me.compareTo(other);
        if (lexicalCompare == 0){
            return o.prob.compareTo(prob);
        }
        return lexicalCompare;
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
        prob.write(dataOutput);
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
        prob.readFields(dataInput);
    }

    /**
     * Override of toString()
     * Used for producing output files of steps
     * @return string 's1 s2 s3 r0 r1' (string values)
     */
    @Override
    public String toString() {
        return word1.toString() + " " + word2.toString() + " " + word3.toString();
    }
}
