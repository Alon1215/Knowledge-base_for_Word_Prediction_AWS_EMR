package Step5;

import Step1.DataPair;
import Step1.Trigram;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StepFive {

    public static class MapClass extends Mapper<Text, Text, GramResult, DoubleWritable> {

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = key.toString().split(" ");
//            String[] prob = value.toString().split(" ");
            double prob = Double.parseDouble(value.toString());
            GramResult trigram = new GramResult(words[0], words[1], words[2],prob);
            context.write(trigram, new DoubleWritable(prob));
        }
    }

    public static class ReducerClass extends Reducer<GramResult, DoubleWritable, GramResult, DoubleWritable> {
        @Override
        protected void reduce(GramResult key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable val : values) {
                context.write(key, val);
            }
        }
    }

    public static class PartitionerClass extends Partitioner<GramResult, DoubleWritable> {
        @Override
        public int getPartition(GramResult trigram, DoubleWritable prob, int numPartitions) {
            return prob.hashCode() % numPartitions;
        }
    }

}
