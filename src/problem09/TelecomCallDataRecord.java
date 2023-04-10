import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TelecomCallDataRecord {
    public static int FROM_PHONE_NUMBER = 0;
    public static int TO_PHONE_NUMBER = 1;
    public static int CALL_START_TIME = 2;
    public static int CALL_END_TIME = 3;
    public static int STD_FLAG = 4;

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "TelecomCallDataRecord");
        job.setJarByClass(TelecomCallDataRecord.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        Text phoneNumber = new Text();
        IntWritable callDuration = new IntWritable();

        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            String[] tokens = line.toString().split("\\|");
            // only process the records with STD flag
            if (!tokens[STD_FLAG].equals("1")) {
                return;
            }
            try {
                Date start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(tokens[CALL_START_TIME]);
                Date end = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(tokens[CALL_END_TIME]);
                callDuration.set((int) (end.getTime() - start.getTime()) / 1000 / 60);
            } catch (ParseException ignored) {
            }
            phoneNumber.set(tokens[FROM_PHONE_NUMBER]);
            context.write(phoneNumber, callDuration);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable result = new IntWritable();
        int sum = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            if (sum >= 60) {
                context.write(key, result);
            }
        }
    }
}
