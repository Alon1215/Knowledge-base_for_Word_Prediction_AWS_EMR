package Step1;

import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 3Gram implementation & representation for the hadoop flow, and assignment logic.
 * Class implements WritableComparable, with the required methods.
 * Trigram is used through the 5 steps, in order to represent a 3gram which is:
 *      Writeable
 *      Comparable
 *      String represented as the given 3gram
 */
public class Trigram implements WritableComparable<Trigram> {


    private final Text word1;
    private final Text word2;
    private final Text word3;


    /**
     * Constructor, input as string
     * @param s first word
     * @param s1 second word
     * @param s2 third word
     */
    public Trigram(String s, String s1, String s2) {
        this.word1 = new Text(s);
        this.word2 = new Text(s1);
        this.word3 = new Text(s2);
    }

    /**
     * Constructor, input as Text
     * @param s first word
     * @param s1 second word
     * @param s2 third word
     */
    public Trigram(Text s, Text s1, Text s2) {
        this.word1 = s;
        this.word2 = s1;
        this.word3 = s2;
    }

    /**
     * Empty constructor
     */
    public Trigram(){
        this.word1 = new Text("");
        this.word2 = new Text("");
        this.word3 = new Text("");
    }

    /**
     * Check if 3gram is equal to other object
     * @param obj input object
     * @return is the 2 objects are the same
     */
    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Trigram && ((Trigram) obj).getWord1().equals(this.word1) && ((Trigram) obj).getWord2().equals(this.word2) && ((Trigram) obj).getWord3().equals(this.word3));
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
     * Compare between to 3grams
     * @param o other 3gram
     * @return int value, result of comparing the 2 3grams
     */
    @Override
    public int compareTo(Trigram o) {
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
    }

    /**
     * Override of toString()
     * Used for producing output files of steps
     * @return string 's1 s2 s3' (string values)
     */
    @Override
    public String toString() {
        return word1.toString() + " " + word2.toString() + " " + word3.toString();
    }
}
