import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class AverageSalary {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "AverageSalary");
        job.setJarByClass(AverageSalary.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<Object, Text, Text, FloatWritable> {
        private final Text name = new Text();
        private final FloatWritable salary = new FloatWritable();

        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(line.toString());
            while (tokenizer.hasMoreTokens()) {
                name.set(tokenizer.nextToken());
                salary.set(Float.parseFloat(tokenizer.nextToken()));
                context.write(name, salary);
            }
        }
    }

    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        float sum = 0;
        int count = 0;

        public void reduce(Text name, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            sum = 0;
            count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            context.write(name, new FloatWritable(sum / count));
        }
    }
}
