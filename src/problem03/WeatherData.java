import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WeatherData {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "WeatherData");
        job.setJarByClass(WeatherData.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            String[] tokens = line.toString().split("\\s+");
            String date = tokens[1].substring(4, 6) + "-" + tokens[1].substring(6, 8) + "-" + tokens[1].substring(0, 4);
            double tempMax = Double.parseDouble(tokens[6].trim());
            double tempMin = Double.parseDouble(tokens[7].trim());
            if (tempMax > 40.0) {
                context.write(new Text(date), new Text("Hot Day"));
            }
            if (tempMin < 10.0) {
                context.write(new Text(date), new Text("Cold Day"));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String temperature = values.iterator().next().toString();
            context.write(key, new Text(temperature));
        }
    }
}
