package Step2;

import Step1.DataPair;
import Step1.Trigram;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
    protected static long N = 0;

    public static class MapClass extends Mapper<Text, Text, Text, Trigram_r1_r2> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            N = context.getConfiguration().getLong("N", -1);
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] gram3 = key.toString().split(" ");
            String[] r0_r1_str = value.toString().split(" ");
            Trigram_r1_r2 tri = new Trigram_r1_r2(gram3[0], gram3[1], gram3[2], Integer.parseInt(r0_r1_str[0]), Integer.parseInt(r0_r1_str[1]));
            context.write(new Text("N_0_" + r0_r1_str[0]), tri);
            context.write(new Text("N_1_" + r0_r1_str[1]), tri);
        }
    }

    public static class CombinerClass extends Reducer<Text,Trigram_r1_r2,Trigram_r1_r2,DataPair> {

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
            for (Trigram_r1_r2 val : values) {
                context.write(val, new DataPair(n_r, t_r));
            }
        }
    }

    public static class ReducerClass extends Reducer<Trigram_r1_r2, DataPair, Trigram, DoubleWritable> {
        @Override
        protected void reduce(Trigram_r1_r2 key, Iterable<DataPair> values, Context context) throws IOException, InterruptedException {
            int total_n_r = 0;
            int total_t_r = 0;
            for (DataPair val : values) {
                total_n_r += val.getFirst().get();
                total_t_r += val.getSecond().get();
            }
            double prob = (double) ((total_t_r) / (N * total_n_r));
            Trigram tri = new Trigram(key.getWord1(), key.getWord2(), key.getWord3());
            context.write(tri, new DoubleWritable(prob));

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

