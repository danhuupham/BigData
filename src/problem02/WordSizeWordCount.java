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
import java.util.StringTokenizer;

public class WordSizeWordCount {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "WordSizeWordCount");
        job.setJarByClass(WordSizeWordCount.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable wordSize = new IntWritable();
        private final Text word = new Text();

        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(line.toString());
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                wordSize.set(word.getLength());
                context.write(wordSize, word);
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        int count = 0;

        public void reduce(IntWritable wordSize, Iterable<Text> words, Context context) throws IOException, InterruptedException {
            count = 0;
            for (Text ignored : words) {
                count++;
            }
            context.write(wordSize, new IntWritable(count));
        }
    }
}
