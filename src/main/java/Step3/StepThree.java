package Step3;

import Step1.DataPair;
import Step1.Trigram;
import Step2.Trigram_r1_r2;
import Step3.TaggedKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
import java.util.LinkedList;

public class StepThree {
    protected static long N = 0;

        // In contrast to ReduceSideJoin, the joined data are written on-the-fly to the context,
        // without aggregating the data in the memory, by using a secondary sort on the tagged keys
        public static class MapClass  extends Mapper<Text,Text, TaggedKey, Text> {

            // The map gets a tagged key and a value and emits the key and the value
            @Override
            public void map(Text key, Text value, Context context) throws IOException,  InterruptedException {
                // case 1: input from job 2
                //<N_{0/1}_{r0/r1}, <N_r, T_r>>
                if (key.toString().startsWith("N_")){
                    context.write(new TaggedKey(-1, key), value);
                } else {
                    // case 2: input from job 1
                    //<Trigram, <r0,r1>>
                    String[] r0_r1 = value.toString().split(" ");
                    context.write(new TaggedKey(1, new Text("N_0_" + r0_r1[0])), key);
                    context.write(new TaggedKey(1, new Text("N_1_" + r0_r1[1])), key);
                }
            }
        }


        public static class ReduceClass  extends Reducer<TaggedKey,Text,Text,DataPair> {

            private int currentTag = 0;
            private String currentKey = "";
            private DataPair current_nr_tr = new DataPair();
            private boolean writeMode = false;

            @Override
            public void reduce(TaggedKey taggedKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                // The reduce gets a tagged key and a set of values
                // In case the first data set (sorted by the tagged key) was completely received,
                // any set of the second dataset is written on-the-fly to the context,
                // by applying the cross product method.
                if (currentKey == null || !currentKey.equals(taggedKey.getKey().toString())) {
                    current_nr_tr = new DataPair(); // TODO CHANGE
                    writeMode = false;
                } else if (currentTag != 0 && currentTag != taggedKey.getTag().get()) writeMode = true;

                if (writeMode)
                    crossProduct(current_nr_tr,values,context);
                else {
                    for (Text value : values) {
                        String[] new_nr_tr = value.toString().split(" ");
//                        System.out.println("value: " + value.toString());
                        current_nr_tr = new DataPair(Integer.parseInt(new_nr_tr[0]), Integer.parseInt(new_nr_tr[1]));
                    }
                }

                currentTag = taggedKey.getTag().get();
                currentKey = taggedKey.getKey().toString();
//                System.out.println("currentTag2 = " + currentTag + ", currentKey2 = " + currentKey);

            }


            protected void crossProduct(DataPair nr_tr,Iterable<Text> table2Values ,Context context) throws IOException, InterruptedException {
                // This specific implementation of the cross product, combine the data of the customers and the orders (
                // of a given costumer id).
                for (Text table2Value : table2Values)
                    context.write(table2Value, nr_tr);
            }
        }

        public static class PartitionerClass extends Partitioner<TaggedKey,Text> {
            // ensure that keys with same key are directed to the same reducer
            @Override
            public int getPartition(TaggedKey key,Text value, int numPartitions) {
                return  (key.getKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
            }
        }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Starting step 3");
        Configuration jobConfiguration = new Configuration();

        Job job3 = Job.getInstance(jobConfiguration);
        //  job5.getConfiguration().setLong("N", job1.getCounters().findCounter(StepOne.Counters.NCounter).getValue());
        job3.setJarByClass(StepThree.class);
        job3.setMapperClass(StepThree.MapClass.class);
        job3.setReducerClass(StepThree.ReduceClass.class);
        job3.setPartitionerClass(StepThree.PartitionerClass.class);
        job3.setMapOutputKeyClass(TaggedKey.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DataPair.class);
        FileInputFormat.addInputPath(job3, new Path(args[1]));
        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("Step Three finished " + job3.waitForCompletion(true));
    }

    }

