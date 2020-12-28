package Step2;

import Step1.DataPair;
import Step1.Trigram;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class StepTwo {


    public static class MapClass extends Mapper<Text, Text, Text, Trigram_r1_r2> {

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            Trigram trigram = null; //TODO: how to parse trigram from key?
            DataPair r0_r1 = null; //TODO: how to parse the pair from value?

            // case 1: input is "N"
            if (trigram.getWord1().toString().equals("N")){ //TODO: set N as a global variable?
                context.getConfiguration().set("N",r0_r1.getFirst().toString());
            } else {

                // case 2: input is 3 gram:
                Trigram_r1_r2 gram = null; //TODO: how to parse the pair from value?
                context.write(new Text("N_0_"+r0_r1.getFirst()), gram);
                context.write(new Text("N_1_"+r0_r1.getSecond()), gram);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Trigram_r1_r2, Trigram, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<Trigram_r1_r2> values, Context context) throws IOException, InterruptedException {
//            /* <N, Occurrences> */
//            if(key.getWord1().toString().equals("N")){
//                int sum = 0;
//                for (DataPair val : values) {
//                    sum += val.getSecond().get();
//                }
//                // TODO : fix its not gonna work remember tom
//                context.getConfiguration().set("N",Integer.toString(sum));
//            } else {
//
//                /* <Trigram, <r_0 r_1>> */
//                int r_0 = 0;
//                int r_1 = 0;
//                for (DataPair val : values) {
//                    int occurrences = val.getSecond().get();
//                    if (val.getFirst().get() == 0) r_0 += occurrences;
//                    else r_1 += occurrences;
//                }
//                context.write(key, new DataPair(r_0, r_1));
//            }
        }
    }
    public static class PartitionerClass extends Partitioner<Trigram, DataPair> {
        @Override
        public int getPartition(Trigram trigram, DataPair counts, int numPartitions) {
            return counts.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        System.out.println("Starting step 1");
//        Configuration jobConfiguration = new Configuration();
//        Job job = Job.getInstance(jobConfiguration);
//        job.setJarByClass(Step1.StepOne.class);
//        job.setMapOutputKeyClass(Step1.StepOne.MapClass.class);
//        job.setCombinerClass(Step1.StepOne.ReducerClass.class);
//        job.setReducerClass(Step1.StepOne.ReducerClass.class);
//        job.setPartitionerClass(Step1.StepOne.PartitionerClass.class);
//        job.setMapOutputKeyClass(Trigram.class);
//        job.setMapOutputValueClass(LongWritable.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

