package Step4;

import Step1.DataPair;
import Step1.Trigram;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


/**
 * Step 4
 * input: outputs of Step 34, text files.
 *      Each line is:
 *      <3gram, <N_0_r, T_0_r>
 *      or
 *      <3gram, <N_1_r, T_1_r>
 *      (each 3gram appears in 2 lines)
 *
 * args: input addresses, output address (in S3)
 *
 * Job flow:
 *      Combine the calculations in the 2 spkits of the corpus,
 *      and arrange the values for the final step & calculations
 *
 * output: text files, whereas for each 3gram:
 *      <3gram, <N', T'>>
 * when:
 *      N' = N_0_r + N_1_r
 *      T' = T_0_r + T_1_r
 * Notice: for each gram, r's value is different for each Corpus.
 */
public class StepFour {

    public static class MapClass extends Mapper<Text, Text, Trigram, DataPair> {

        /**
         * Mapper class implementation for Step 4
         * output: <TriGram, <N_0_r, T_0_r>> or <TriGram, <N_0_r, T_0_r>>
         */
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] gram = key.toString().split(" ");
            String[] data = value.toString().split(" ");
            Trigram tri = new Trigram(gram[0], gram[1], gram[2]);
            context.write(tri, new DataPair(Integer.parseInt(data[0]), Integer.parseInt(data[1])));
        }
    }

    /**
     * Reducer class implementation for Step 4
     * output: text files, whereas for each 3gram:
     *      <3gram, <N', T'>
     * when:
     *      N' = N_0_r + N_1_r
     *      T' = T_0_r + T_1_r
     * Notice: for each gram, r's value is different for each Corpus.
     */
    public static class ReducerClass extends Reducer<Trigram, DataPair, Trigram, DataPair> {
        @Override
        protected void reduce(Trigram key, Iterable<DataPair> values, Context context) throws IOException, InterruptedException {
            int total_n_r = 0;
            int total_t_r = 0;
            for (DataPair val : values) {
                total_n_r += val.getFirst().get();
                total_t_r += val.getSecond().get();
            }
            context.write(key, new DataPair(total_n_r, total_t_r));
        }
    }

    /**
     * Partitioner implementation for step 4
     * Split map result to reducers,
     * based on the 3gram
     */
    public static class PartitionerClass extends Partitioner<Trigram, DataPair> {
        @Override
        public int getPartition(Trigram trigram, DataPair counts, int numPartitions) {
            return (trigram.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    /**
     * Main method of step 4
     * Initiate and configure job 4,
     * Start it's running on the input file,
     * after run is completed successfully, upload output to S3
     * finish Step 4 run.
     *
     * @param args input address, output destination
     * @throws IOException if input doesn't exist
     * @throws ClassNotFoundException if Classes of the job specification (Mapper, Reducer ...) is not defined well.
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration jobConfiguration = new Configuration();

        System.out.println("Starting step 4");
        Job job4 = Job.getInstance(jobConfiguration);
        job4.setJarByClass(StepFour.class);
        job4.setMapperClass(StepFour.MapClass.class);
        if(args[1].equals("1"))
            job4.setCombinerClass(StepFour.ReducerClass.class);
        job4.setReducerClass(StepFour.ReducerClass.class);
        job4.setPartitionerClass(StepFour.PartitionerClass.class);
        job4.setMapOutputKeyClass(Trigram.class);
        job4.setMapOutputValueClass(DataPair.class);
        job4.setOutputKeyClass(Trigram.class);
        job4.setOutputValueClass(DataPair.class);
        FileInputFormat.addInputPath(job4, new Path(args[2]));
        FileOutputFormat.setOutputPath(job4, new Path(args[3]));
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("Step Four finished " + job4.waitForCompletion(true));
    }
}

