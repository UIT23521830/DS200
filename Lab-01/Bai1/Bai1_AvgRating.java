import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Bai1_AvgRating {

    // ============ MAPPER ============
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

        private IntWritable movieId = new IntWritable();
        private DoubleWritable rating = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.trim().isEmpty()) return;

            String[] parts = line.split(", ");

            movieId.set(Integer.parseInt(parts[1]));
            rating.set(Double.parseDouble(parts[2]));

            context.write(movieId, rating);
        }
    }

    // ============ REDUCER ============
    public static class MyReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            double avg = sum / count;

            context.write(key, new DoubleWritable(avg));
        }
    }

    // ============ MAIN ============
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Rating");

        job.setJarByClass(Bai1_AvgRating.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}