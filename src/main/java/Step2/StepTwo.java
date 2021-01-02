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

    public static class MapClass extends Mapper<Text, Text, Text, Trigram_r1_r2> {

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] gram3 = key.toString().split(" ");
            String[] r0_r1_str = value.toString().split(" ");
            Trigram_r1_r2 tri = new Trigram_r1_r2(gram3[0], gram3[1], gram3[2], Integer.parseInt(r0_r1_str[0]), Integer.parseInt(r0_r1_str[1]));
            context.write(new Text("N_0_" + r0_r1_str[0]), tri);
            context.write(new Text("N_1_" + r0_r1_str[1]), tri);
//            System.out.println("map trigram: " + tri.toString());
//            System.out.println("map datapair: " + value.toString());
        }
    }

    public static class ReducerClass extends Reducer<Text,Trigram_r1_r2,Text,DataPair> {

        @Override
        public void reduce(Text key, Iterable<Trigram_r1_r2> values, Context context) throws IOException, InterruptedException {
//            System.out.println("At Reducer class on Step two");
            int n_r = 0;
            int t_r = 0;
            if(key.toString().charAt(2) == '0') {
                for (Trigram_r1_r2 val : values) {
                    n_r++;
//                    System.out.println("reducer step two case one");
                    t_r += val.getR1().get();
                  //  context.write(new Text(val.getWord1() + " " + val.getWord2() + " " + val.getWord3()), new DataPair(val.getR0().get(), val.getR1().get()));
                }
            }
            else {
                for (Trigram_r1_r2 val : values) {
                    n_r++;
//                    System.out.println("reducer step two case two");
                    t_r += val.getR0().get();
                    //context.write(new Text(val.getWord1() + " " + val.getWord2() + " " + val.getWord3()), new DataPair(val.getR0().get(), val.getR1().get()));
                }
            }
            context.write(new Text(key.toString()), new DataPair(n_r, t_r));
        }
    }


    public static class PartitionerClass extends Partitioner<Trigram, DataPair> {
        @Override
        public int getPartition(Trigram trigram, DataPair counts, int numPartitions) {
            return counts.hashCode() % numPartitions;
        }
    }

}

