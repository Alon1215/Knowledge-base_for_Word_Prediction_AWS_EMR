package Step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

    public static class MapClass extends Mapper<LongWritable, Text, Trigram, DataPair>{



        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t"); //parse the line components
            String[] gram3 = line[0].split("\\s+"); // parse gram
            int occurrences = Integer.parseInt(line[2]); // parse the gram occurrences
            int group = (int) Math.round( Math.random() ); // randomly set gram's group 0/1
            context.write(new Trigram(gram3[0], gram3[1], gram3[2]),  new DataPair(group , occurrences));
            context.write(new Trigram("N","",""), new DataPair(occurrences,0));

        }
    }
    public static class ReducerClass extends Reducer<Trigram, DataPair, Trigram, DataPair>{
        @Override
        protected void reduce(Trigram key, Iterable<DataPair> values, Context context) throws IOException, InterruptedException {
            /* <N, Occurrences> */
            if(key.getWord1().toString().equals("N")){
                int sum = 0;
                for (DataPair val : values) {
                    sum += val.getSecond().get();
                }
                // TODO : fix its not gonna work remember tom
//                context.getConfiguration().set("N",Integer.toString(sum));
                context.write(key,  new DataPair(sum,0)); // TODO : temporary implementation CHANGE
            } else {

                /* <Trigram, <r_0 r_1>> */
                int r_0 = 0;
                int r_1 = 0;
                for (DataPair val : values) {
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
    }
    public static class PartitionerClass extends Partitioner<Trigram, DataPair>{
        @Override
        public int getPartition(Trigram trigram, DataPair counts, int numPartitions) {
            return counts.hashCode() % numPartitions;
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
        job.setMapOutputValueClass(DataPair.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
