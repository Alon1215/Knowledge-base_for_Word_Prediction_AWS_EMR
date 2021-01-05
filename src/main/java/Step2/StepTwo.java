package Step2;

import Step1.DataPair;
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
 * Step 2
 * input: output of Step 1, text file.
 *      Each line is <3gram, r0_r1>
 *
 * args: input address, output address (in S3)
 *
 * Job flow:
 *      Calculate for every r:
 *          N_0_r & T_0_r
 *          N_0_r & T_0_r .
 *      Reducer sums up for every r the number of 3grams in the given Corpus,
 *      and the occurrences of those 3grams in the other Corpus.
 *
 * output: text files, whereas each r:
 *      <"N_0_r", <N_0_r, T_0_r>> or <"N_0_r", <N_1_r, T_1_r>>
 * when:
 *      "N_0_r" = string represent of the r & Corpus
 *      <N_0_r, T_0_r> = as defined in the assignment
 *      <N_1_r, T_1_r> = Same as above
 */
public class StepTwo {


    /**
     * Mapper class implementation for Step 2
     * output: <"N_0_r", <TriGram, r0_r1>> or <"N_0_r", <TriGram, r0_r1>>
     * CorpusNum set randomly to 0 or 1
     */
    public static class MapClass extends Mapper<Text, Text, Text, Trigram_r1_r2> {

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] gram3 = key.toString().split(" ");
            String[] r0_r1_str = value.toString().split(" ");
            Trigram_r1_r2 tri = new Trigram_r1_r2(gram3[0], gram3[1], gram3[2], Integer.parseInt(r0_r1_str[0]), Integer.parseInt(r0_r1_str[1]));
            context.write(new Text("N_0_" + r0_r1_str[0]), tri);
            context.write(new Text("N_1_" + r0_r1_str[1]), tri);
        }
    }


    /**
     * Reducer class implementation for Step 2
     * output: text files, whereas for each r:
     *      <"N_0_r", <N_0_r, T_0_r> or <"N_0_r", <N_1_r, T_1_r>>
     * when:
     *      "N_0_r" = string represent of the r & Corpus
     *      <N_0_r, T_0_r> = as defined in the assignment
     *      <N_1_r, T_1_r> = Same as above

     */
    public static class ReducerClass extends Reducer<Text,Trigram_r1_r2,Text,DataPair> {

        @Override
        public void reduce(Text key, Iterable<Trigram_r1_r2> values, Context context) throws IOException, InterruptedException {
            int n_r = 0;
            int t_r = 0;
            if(key.toString().charAt(2) == '0') {
                for (Trigram_r1_r2 val : values) {
                    n_r++;
                    t_r += val.getR1().get();
                }
            }
            else {
                for (Trigram_r1_r2 val : values) {
                    n_r++;
                    t_r += val.getR0().get();
                }
            }
            context.write(key, new DataPair(n_r, t_r));
        }
    }


    /**
     * Partitioner implementation for step 2
     * Split map result to reducers,
     * based on n_${0 or 1}_r
     */
    public static class PartitionerClass extends Partitioner<Text, Trigram_r1_r2> {
        @Override
        public int getPartition(Text n_01_r, Trigram_r1_r2 trigram_r1_r2, int numPartitions) {
            return (n_01_r.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    /**
     * Main method of step 2
     * Initiate and configure job 2,
     * Start it's running on the input file,
     * after run is completed successfully, upload output to S3
     * finish Step 2 run.
     *
     * @param args input address, output destination
     * @throws IOException if input doesn't exist
     * @throws ClassNotFoundException if Classes of the job specification (Mapper, Reducer ...) is not defined well.
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Starting step 2");
        Configuration jobConfiguration = new Configuration();
        Job job2 = Job.getInstance(jobConfiguration);
        job2.setJarByClass(StepTwo.class);
        job2.setMapperClass(StepTwo.MapClass.class);
        job2.setReducerClass(StepTwo.ReducerClass.class);
        job2.setPartitionerClass(StepTwo.PartitionerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Trigram_r1_r2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DataPair.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("Step Two finished " + job2.waitForCompletion(true));
    }
}
