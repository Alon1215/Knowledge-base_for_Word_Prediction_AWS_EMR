package Step5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * Step 5
 * input: outputs of Step 4, text files.
 *      Each line is:
 *      <3gram, <N', T'>>
 *
 * args: input addresses, output address (in S3)
 *
 * Job flow:
 *      Final step of the assignment.
 *      Retrieve N from Step 1, and calculate for each the 3gram his value.
 *      create the final output file.
 *
 * output: text files, whereas for each 3gram:
 *      <3gram, probability>
 * when:
 *      probability = T' / (N' * N)
 *
 * Output is sorted as required in the assignment.
 */
public class StepFive {

    protected static long N = -1;
    public static final String BUCKET_NAME = "s3://dsp211emr/";

    /**
     * Mapper class implementation for Step 5
     * Setup: retrieve N value from Step 1.
     *
     * output: <GramResult, <N', T'>
     * GramResult := same as 3gram, but is sorted as required in output file
     */
    public static class MapClass extends Mapper<Text, Text, GramResult, DoubleWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            N = context.getConfiguration().getLong("N", -1);
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = key.toString().split(" ");
            String[] nr_tr = value.toString().split(" ");
            double total_n_r = Double.parseDouble(nr_tr[0]);
            double total_t_r = Double.parseDouble(nr_tr[1]);
            double prob =  ((total_t_r) / (N * total_n_r));
            GramResult trigram = new GramResult(words[0], words[1], words[2],prob);
            context.write(trigram, new DoubleWritable(prob));
        }
    }

    /**
     * Reducer class implementation for Step 5
     * output: text files, whereas for each 3gram:
     *      <3gram, probability>
     * when:
     *      probability = T' / (N' * N)
     *
     * Output is sorted as required in the assignment.
     */
    public static class ReducerClass extends Reducer<GramResult, DoubleWritable, GramResult, DoubleWritable> {
        @Override
        protected void reduce(GramResult key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable val : values) {
                context.write(key, val);
            }
        }
    }

    /**
     * Partitioner implementation for step 5
     * Split map result to reducers,
     * based on the GramResult
     */
    public static class PartitionerClass extends Partitioner<GramResult, DoubleWritable> {
        @Override
        public int getPartition(GramResult trigram, DoubleWritable prob, int numPartitions) {
            return (trigram.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    /**
     * Main method of step 5
     * Initiate and configure job 5,
     * Start it's running on the input file,
     * after run is completed successfully, upload final output to S3
     * finish Step 5 run, and all the assignment flow.
     *
     * @param args input address, output destination
     * @throws IOException if input doesn't exist
     * @throws ClassNotFoundException if Classes of the job specification (Mapper, Reducer ...) is not defined well.
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration jobConfiguration = new Configuration();

        System.out.println("Starting step 5");
        Job job5 = Job.getInstance(jobConfiguration);

        FileSystem fs = FileSystem.get(URI.create(BUCKET_NAME), job5.getConfiguration());
        FSDataInputStream fsDataInputStream = fs.open(new Path("s3://dsp211emr/counters_output.txt"));
        BufferedReader d = new BufferedReader(new InputStreamReader(fsDataInputStream));
        job5.getConfiguration().setLong("N", Long.parseLong(d.readLine()));

        job5.setJarByClass(StepFive.class);
        job5.setMapperClass(StepFive.MapClass.class);
        job5.setReducerClass(StepFive.ReducerClass.class);
        job5.setPartitionerClass(StepFive.PartitionerClass.class);
        job5.setMapOutputKeyClass(GramResult.class);
        job5.setMapOutputValueClass(DoubleWritable.class);
        job5.setOutputKeyClass(GramResult.class);
        job5.setOutputValueClass(DoubleWritable.class);
        job5.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job5, new Path(args[1]));
        FileOutputFormat.setOutputPath(job5, new Path(args[2]));
        job5.setInputFormatClass(KeyValueTextInputFormat.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("Step five finished " + job5.waitForCompletion(true));


    }
}
