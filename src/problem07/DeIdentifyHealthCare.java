import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;

public class DeIdentifyHealthCare {
    private static final byte[] encryptKey = "dandandanhuupham".getBytes();
    public static Integer[] encryptColumns = {2, 3, 4, 5, 6, 8};
    static int counter;
    static String token;
    static StringBuilder stringBuilder;

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "DeIdentifyHealthCare");
        job.setJarByClass(DeIdentifyHealthCare.class);

        job.setMapperClass(Map.class);

        job.setMapOutputKeyClass(NullWritable.class);

        job.setOutputKeyClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static String encrypt(String str, byte[] encryptKey) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        SecretKeySpec secretKeySpec = new SecretKeySpec(encryptKey, "AES");
        cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);
        return Base64.encodeBase64String(cipher.doFinal(str.getBytes())).trim();
    }

    public static class Map extends Mapper<Object, Text, NullWritable, Text> {
        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            // line = "PatientID,Name,DOB,Phone Number,Email_Address,SSN,Gender,Disease,weight"
            StringTokenizer tokenizer = new StringTokenizer(line.toString(), ",");
            ArrayList<Integer> list = new ArrayList<>();
            Collections.addAll(list, encryptColumns);

            stringBuilder = new StringBuilder();
            counter = 1;

            while (tokenizer.hasMoreTokens()) {
                token = tokenizer.nextToken();
                if (list.contains(counter)) {
                    if (stringBuilder.length() > 0) {
                        stringBuilder.append(",");
                    }
                    try {
                        stringBuilder.append(encrypt(token, encryptKey));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    if (stringBuilder.length() > 0) {
                        stringBuilder.append(",");
                    }
                    stringBuilder.append(token);
                }
                counter++;
            }
            context.write(NullWritable.get(), new Text(stringBuilder.toString()));
        }
    }
}
