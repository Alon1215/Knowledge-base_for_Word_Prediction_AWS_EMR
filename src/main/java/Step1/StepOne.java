package Step1;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.mortbay.util.StringUtil;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

public class StepOne {
    public enum Counters { NCounter }
    public static final String BUCKET_NAME = "s3://dsp211emr/";
    private static final char HEBREW_FIRST = (char) 1488;
    private static final char HEBREW_LAST = (char) 1514;

    public static class MapClass extends Mapper<LongWritable, Text, Trigram, DataPair>{

        protected boolean isLegalTrigram(String[] s){
            for(String str: s) {
                for (int i = 0; i < str.length(); i++) {
                    char c = str.charAt(i);
                    if (c != ' ' && (c < HEBREW_FIRST || c > HEBREW_LAST))
                        return false;
                }
            }
            return true;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(value.toString().equals(""))
                return;
            String[] line = value.toString().split("\t"); //parse the line components
            String[] gram3 = line[0].split(" "); // parse gram
            if(gram3.length != 3 || !isLegalTrigram(gram3))
                return;
//            if(!StringUtils.isNumeric(line[2]) || line[2].equals(""))
//                return;
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

    public static class PartitionerClass extends Partitioner<Trigram, DataPair>{
        @Override
        public int getPartition(Trigram trigram, DataPair dataPair, int numPartitions) {
            return dataPair.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Starting step 1");
        Configuration jobConfiguration = new Configuration();

        Job job1 = Job.getInstance(jobConfiguration);
        job1.setJarByClass(StepOne.class);
        job1.setMapperClass(StepOne.MapClass.class);
        job1.setReducerClass(StepOne.ReducerClass.class);
        job1.setPartitionerClass(StepOne.PartitionerClass.class);
        job1.setMapOutputKeyClass(Trigram.class);
        job1.setMapOutputValueClass(DataPair.class);
        job1.setOutputKeyClass(Trigram.class);
        job1.setOutputValueClass(DataPair.class);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
     //   MultipleOutputs.addNamedOutput(job1,);
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("Step one finished " + job1.waitForCompletion(true));
//        jobConfiguration.setLong("N", job1.getCounters().findCounter(Counters.NCounter).getValue());

        FileSystem fs = FileSystem.get(URI.create(BUCKET_NAME), job1.getConfiguration());
        FSDataOutputStream fsDataOutputStream = fs.create(new Path(BUCKET_NAME + "counters_output.txt"));
        PrintWriter writer = new PrintWriter(fsDataOutputStream);
  //      char[] b = ByteBuffer.allocate(Long.BYTES).putLong(job1.getCounters().findCounter(Counters.NCounter).getValue()).asCharBuffer().array();
        writer.write(Long.toString(job1.getCounters().findCounter(Counters.NCounter).getValue()));
        writer.close();
        fsDataOutputStream.close();
        fs.close();
    }

}
