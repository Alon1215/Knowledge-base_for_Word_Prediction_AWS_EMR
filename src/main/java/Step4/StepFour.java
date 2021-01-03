package Step4;

import Step1.DataPair;
import Step1.StepOne;
import Step1.Trigram;
import Step2.Trigram_r1_r2;
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class StepFour {
    protected static long N = 0;

    public static class MapClass extends Mapper<Text, Text, Trigram, DataPair> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            N = context.getConfiguration().getLong("N", -1);
            System.out.println("Setup, N=" + N);
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] gram = key.toString().split(" ");
            String[] data = value.toString().split(" ");
            Trigram tri = new Trigram(gram[0], gram[1], gram[2]);
            context.write(tri, new DataPair(Integer.parseInt(data[0]), Integer.parseInt(data[1])));
        }
    }

    public static class ReducerClass extends Reducer<Trigram, DataPair, Trigram, DoubleWritable> {
        @Override
        protected void reduce(Trigram key, Iterable<DataPair> values, Context context) throws IOException, InterruptedException {
            double total_n_r = 0;
            double total_t_r = 0;
            for (DataPair val : values) {
                total_n_r += val.getFirst().get();
                total_t_r += val.getSecond().get();
            }
            double prob =  ((total_t_r) / (N * total_n_r));
            Trigram tri = new Trigram(key.getWord1(), key.getWord2(), key.getWord3());
            context.write(tri, new DoubleWritable(prob));
//            System.out.println("reducer: " + tri.toString() + ", " + prob);
        }
    }

    public static class PartitionerClass extends Partitioner<Trigram, DoubleWritable> {
        @Override
        public int getPartition(Trigram trigram, DoubleWritable counts, int numPartitions) {
            return counts.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration jobConfiguration = new Configuration();

        System.out.println("Starting step 4");
        Job job4 = Job.getInstance(jobConfiguration);
       // job4.getConfiguration().setLong("N", job1.getCounters().findCounter(StepOne.Counters.NCounter).getValue());
        job4.setJarByClass(StepFour.class);
        job4.setMapperClass(StepFour.MapClass.class);
        job4.setReducerClass(StepFour.ReducerClass.class);
        job4.setPartitionerClass(StepFour.PartitionerClass.class);
        job4.setMapOutputKeyClass(Trigram.class);
        job4.setMapOutputValueClass(DataPair.class);
        job4.setOutputKeyClass(Trigram.class);
        job4.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job4, new Path(args[1]));
        FileOutputFormat.setOutputPath(job4, new Path(args[2]));
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("Step Four finished " + job4.waitForCompletion(true));
    }
}

