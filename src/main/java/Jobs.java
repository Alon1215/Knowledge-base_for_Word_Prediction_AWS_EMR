import Step1.DataPair;
import Step1.StepOne;
import Step1.Trigram;
import Step2.StepTwo;
import Step2.Trigram_r1_r2;
import Step3.StepThree;
import Step3.TaggedKey;
import Step4.StepFive;
import Step5.GramResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
    
        //job5 1
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
        job2.setJarByClass(StepTwo.class);
        job2.setMapperClass(StepTwo.MapClass.class);
        job2.setReducerClass(StepTwo.ReducerClass.class);
        job2.setPartitionerClass(StepTwo.PartitionerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Trigram_r1_r2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DataPair.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("Step Two finished " + job2.waitForCompletion(true));

        //job 3
        System.out.println("Starting step 3");
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


        //job 4
        System.out.println("Starting step 4");
        Job job4 = Job.getInstance(jobConfiguration);
        job4.getConfiguration().setLong("N", job1.getCounters().findCounter(StepOne.Counters.NCounter).getValue());
        job4.setJarByClass(StepFive.class);
        job4.setMapperClass(StepFive.MapClass.class);
        job4.setReducerClass(StepFive.ReducerClass.class);
        job4.setPartitionerClass(StepFive.PartitionerClass.class);
        job4.setMapOutputKeyClass(Trigram.class);
        job4.setMapOutputValueClass(DataPair.class);
        job4.setOutputKeyClass(Trigram.class);
        job4.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job4, new Path(args[3]));
        FileOutputFormat.setOutputPath(job4, new Path(args[4]));
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("Step Four finished " + job4.waitForCompletion(true));

        //job 5
        System.out.println("Starting step 5");
        Job job5 = Job.getInstance(jobConfiguration);
        job5.setJarByClass(StepFive.class);
        job5.setMapperClass(StepFive.MapClass.class);
        job5.setReducerClass(StepFive.ReducerClass.class);
        job5.setPartitionerClass(StepFive.PartitionerClass.class);
        job5.setMapOutputKeyClass(GramResult.class);
        job5.setMapOutputValueClass(DoubleWritable.class);
        job5.setOutputKeyClass(GramResult.class);
        job5.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job5, new Path(args[4]));
        FileOutputFormat.setOutputPath(job5, new Path(args[5]));
        job5.setInputFormatClass(KeyValueTextInputFormat.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("Step five finished " + job5.waitForCompletion(true));
    }
}
