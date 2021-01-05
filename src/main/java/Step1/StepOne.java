package Step1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;


/**
 * Step 1
 * input: a sequential file (hebrew, 3gram)
 * args: isLocalAggregation, input address, output address (in AWS)
 *
 * Job flow:
 *      Clean input from invalid Grams
 *      Split the corpus to 2 splits, and for each 3gram, sums up occurrences in each Corpus
 *
 * output: text files, whereas each line:
 *      <3gram,r0_r1>
 * when:
 *      3gram = legal hebrew tuple from input
 *      r_0 = occurrences in corpus 0
 *      r_1 = occurrences in corpus 1

 */
public class StepOne {
    public enum Counters { NCounter }
    public static final String BUCKET_NAME = "s3://dsp211emr/";
    private static final char HEBREW_FIRST = (char) 1488;
    private static final char HEBREW_LAST = (char) 1514;

    /**
     * Mapper class implementation for Step 1
     * output: <3gram, CorpusNum_occurrences>
     * CorpusNum set randomly to 0 or 1
     */
    public static class MapClass extends Mapper<LongWritable, Text, Trigram, DataPair>{

        protected boolean isLegalTrigram(String[] s){
            for(String str: s) {
                for (int i = 0; i < str.length(); i++) {
                    char c = str.charAt(i);
                    if (c != ' ' && (c < HEBREW_FIRST || c > HEBREW_LAST))
                        return false;
                }
            }
            return true;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(value.toString().equals(""))
                return;
            String[] line = value.toString().split("\t"); //parse the line components
            String[] gram3 = line[0].split(" "); // parse gram
            if(gram3.length != 3 || !isLegalTrigram(gram3))
                return;
            int occurrences = Integer.parseInt(line[2]); // parse the gram occurrences
            int group = (int) Math.round(Math.random()); // randomly set gram's group 0/1
            context.write(new Trigram(gram3[0], gram3[1], gram3[2]),  new DataPair(group , occurrences));
            context.getCounter(Counters.NCounter).increment(occurrences);
        }
    }

    /**
     * Reducer class implementation for Step 1
     * output: <3gram, r0_r1>
     * r0 = occurrences of the given 3gram in Corpus 0
     * r1 is defined respectively
     */
    public static class ReducerClass extends Reducer<Trigram, DataPair, Trigram, DataPair>{
        @Override
        protected void reduce(Trigram key, Iterable<DataPair> values, Context context) throws IOException, InterruptedException {
            /* <Trigram, <r_0 r_1>> */
            int r_0 = 0;
            int r_1 = 0;
            for (DataPair val : values) {
                int occurrences = val.getSecond().get();
                if (val.getFirst().get() == 0) {
                    r_0 += occurrences;
                }
                else {
                    r_1 += occurrences;
                }
            }
            context.write(key, new DataPair(r_0, r_1));
        }
    }


    /**
     * Partitioner implementation for step 1
     * Split map result to reducers,
     * based on DataPair
     */
    public static class PartitionerClass extends Partitioner<Trigram, DataPair>{
        @Override
        public int getPartition(Trigram trigram, DataPair dataPair, int numPartitions) {
            return (dataPair.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    /**
     * Main method of step 1
     * Initiate and configure job 1,
     * Start it's running on the input file,
     * after run is completed successfully, upload N to S3, and output files.
     * finish Step 1 run.
     *
     * @param args isLocalAggregation, input address, output destination
     * @throws IOException if input doesn't exist
     * @throws ClassNotFoundException if Classes of the job specification (Mapper, Reducer ...) is not defined well.
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Starting step 1");
        Configuration jobConfiguration = new Configuration();

        Job job1 = Job.getInstance(jobConfiguration);
        job1.setJarByClass(StepOne.class);
        job1.setMapperClass(StepOne.MapClass.class);
        job1.setReducerClass(StepOne.ReducerClass.class);
        if(args[1].equals("1"))
            job1.setCombinerClass(StepOne.ReducerClass.class);
        job1.setPartitionerClass(StepOne.PartitionerClass.class);
        job1.setMapOutputKeyClass(Trigram.class);
        job1.setMapOutputValueClass(DataPair.class);
        job1.setOutputKeyClass(Trigram.class);
        job1.setOutputValueClass(DataPair.class);
        FileInputFormat.addInputPath(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("Step one finished " + job1.waitForCompletion(true));
        FileSystem fs = FileSystem.get(URI.create(BUCKET_NAME), job1.getConfiguration());
        FSDataOutputStream fsDataOutputStream = fs.create(new Path(BUCKET_NAME + "counters_output.txt"));
        PrintWriter writer = new PrintWriter(fsDataOutputStream);
        writer.write(Long.toString(job1.getCounters().findCounter(Counters.NCounter).getValue()));
        writer.close();
        fsDataOutputStream.close();
        fs.close();
    }

}
