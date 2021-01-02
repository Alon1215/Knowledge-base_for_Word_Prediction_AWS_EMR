package Step4;

import Step1.DataPair;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class StepFive {
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
            String[] t = key.toString().split(" ");
            String[] d = value.toString().split(" ");
            Trigram tri = new Trigram(t[0], t[1], t[2]);
            context.write(tri, new DataPair(Integer.parseInt(d[0]), Integer.parseInt(d[1])));
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

}

