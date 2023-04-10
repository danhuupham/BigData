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

public class MusicTrack {
    public static final int USER_ID = 0;
    public static final int TRACK_ID = 1;
    public static final int IS_SHARED = 2;
    public static final int RADIO = 3;
    public static final int IS_SKIPPED = 4;

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "MusicTrack");
        job.setJarByClass(MusicTrack.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            String[] tokens = line.toString().split("\\|");
            int userId = Integer.parseInt(tokens[USER_ID]);
            String trackId = tokens[TRACK_ID];
            String isShared = tokens[IS_SHARED];
            String radio = tokens[RADIO];
            String isSkipped = tokens[IS_SKIPPED];

            context.write(new IntWritable(userId), new Text(trackId + " " + isShared + " " + radio + " " + isSkipped));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        int shared = 0;
        int radio = 0;
        int total = 0;
        int skipped = 0;

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            shared = 0;
            radio = 0;
            total = 0;
            skipped = 0;

            for (Text value : values) {
                String[] tokens = value.toString().split(" ");

                String isShared = tokens[IS_SHARED - 1];
                String isRadio = tokens[RADIO - 1];
                String isSkipped = tokens[IS_SKIPPED - 1];

                shared += Integer.parseInt(isShared);
                radio += Integer.parseInt(isRadio);
                total++;
                skipped += Integer.parseInt(isSkipped);
            }

            context.write(key, new Text(shared + " " + radio + " " + total + " " + skipped));
        }
    }
}