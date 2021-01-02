package Step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class StepOne {
    public enum Counters { NCounter }

    public static class MapClass extends Mapper<LongWritable, Text, Trigram, DataPair>{


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(value.toString().equals(""))
                return;
            String[] line = value.toString().split("\t"); //parse the line components
            String[] gram3 = line[0].split(" "); // parse gram
            if(gram3.length != 3)
                return;
            int occurrences = Integer.parseInt(line[2]); // parse the gram occurrences
            int group = (int) Math.round(Math.random()); // randomly set gram's group 0/1
//            System.out.println("Gram " + line[0] + " group " + group);
            context.write(new Trigram(gram3[0], gram3[1], gram3[2]),  new DataPair(group , occurrences));
            context.getCounter(Counters.NCounter).increment(occurrences);
        }
    }
    public static class ReducerClass extends Reducer<Trigram, DataPair, Trigram, DataPair>{
        @Override
        protected void reduce(Trigram key, Iterable<DataPair> values, Context context) throws IOException, InterruptedException {
            /* <Trigram, <r_0 r_1>> */
            int r_0 = 0;
            int r_1 = 0;
            for (DataPair val : values) {
//                System.out.println("First " + val.getFirst().get() + " Second "+ val.getSecond().get());
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

//    public static class CombinerClass extends Reducer<K2,V2,K3,V3> {
//
//        @Override
//        public void reduce(K2 key, Iterable<V2> values, Context context) throws IOException,  InterruptedException {
//        }
//    }

    public static class PartitionerClass extends Partitioner<Trigram, DataPair>{
        @Override
        public int getPartition(Trigram trigram, DataPair counts, int numPartitions) {
            return counts.hashCode() % numPartitions;
        }
    }


}
