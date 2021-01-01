import Step1.DataPair;
import Step1.StepOne;
import Step1.Trigram;
import Step2.StepTwo;
import Step2.Trigram_r1_r2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Jobs {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    
        //job2 1
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
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("Step one finished " + job1.waitForCompletion(true));

        //job 2

        System.out.println("Starting step 2");

        Job job2 = Job.getInstance(jobConfiguration);
        job2.getConfiguration().setLong("N", job1.getCounters().findCounter(StepOne.Counters.NCounter).getValue());
        job2.setJarByClass(StepTwo.class);
        job2.setMapperClass(StepTwo.MapClass.class);
        job2.setCombinerClass(StepTwo.CombinerClass.class);
        job2.setReducerClass(StepTwo.ReducerClass.class);
        job2.setPartitionerClass(StepTwo.PartitionerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Trigram_r1_r2.class);
        job2.setOutputKeyClass(Trigram.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("Step Two finished " + job2.waitForCompletion(true));

    }
}
