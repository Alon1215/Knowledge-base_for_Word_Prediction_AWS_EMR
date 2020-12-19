package Step_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.*;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class StepOne {

    public static String[] hebrew_array = {"", "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן",
            "מכל", "מי", "מהם", "מה", "מ", "למה", "לכל", "לי", "לו", "להיות", "לה",
            "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר", "יד", "י", "זה", "ז", "ועל", "ומי",
            "ולא", "וכן", "וכל", "והיא",
            "והוא", "ואם", "ו", "הרבה", "הנה", "היו", "היה", "היא", "הזה",
            "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם", "אלה",
            "אל", "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".", "-", "*",
            "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם",
            "לנו", "להם", "ישראל", "יודע", "זאת", "השמים", "הזאת", "הדברים", "הדבר",
            "הבית", "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם", "אדם", "(", "חלק",
            "שני", "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם", "לפי", "ל",
            "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן",
            "היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א"
    };

    public static class MapClass extends Mapper<LongWritable, Text, Trigram, IntWritable>{



        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String[] gram3 = line[0].split("\\s+");
            IntWritable occurrences = new IntWritable(Integer.parseInt(line[2]));
            context.write(new Trigram(gram3[0], gram3[1], gram3[2]), occurrences);
//            context.write(new Trigram( new Text(gram3[0]), new Text(gram3[1]), new Text(gram3[2])), new LongWritable(1));

        }
    }
    public static class ReducerClass extends Reducer<Trigram, IntWritable, Trigram, IntWritable>{
        @Override
        protected void reduce(Trigram key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val: values){
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    public static class PartitionerClass extends Partitioner<Trigram, IntWritable>{
        @Override
        public int getPartition(Trigram trigram, IntWritable count, int numPartitions) {
            return count.get() % numPartitions;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Starting step 1");
        Configuration jobConfiguration = new Configuration();
        Job job = Job.getInstance(jobConfiguration);
        job.setJarByClass(StepOne.class);
        job.setMapOutputKeyClass(MapClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setMapOutputKeyClass(Trigram.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setMinInputSplitSize(job, 2);
        FileInputFormat.setMaxInputSplitSize(job, 2);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
