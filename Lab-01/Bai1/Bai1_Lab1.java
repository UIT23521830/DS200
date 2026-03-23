import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Bai1_Lab1 {

    // ===== Mapper =====
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

        private IntWritable movieId = new IntWritable();
        private DoubleWritable rating = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty()) return;

            String[] parts = line.split(",");

            // debug
            System.out.println("LINE: " + line);

            if (parts.length < 3) return;

            // Bỏ qua dòng header (nếu có)
            if (!parts[0].trim().matches("-?\\d+")) return;

            try {
                int mId = Integer.parseInt(parts[1].trim());
                double r = Double.parseDouble(parts[2].trim());

                movieId.set(mId);
                rating.set(r);

                context.write(movieId, rating);

            } catch (Exception e) {
                System.out.println("ERROR: " + line);
            }
        }
    }

    // ===== Reducer =====
    public static class MyReducer extends Reducer<IntWritable, DoubleWritable, Text, Text> {

        private HashMap<Integer, String> movieMap = new HashMap<>();

        private String maxMovie = "";
        private double maxRating = 0;

        // đọc movies.txt
        protected void setup(Context context) throws IOException {

            Path path = new Path("/input/movies.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

            String line;
            while ((line = br.readLine()) != null) {

                String[] parts = line.split(",");

                if (parts.length < 2) continue;

                int id = Integer.parseInt(parts[0].trim());
                String name = parts[1].trim();

                movieMap.put(id, name);
            }

            br.close();
        }

        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count == 0) return;

            double avg = sum / count;

            String movieName = movieMap.get(key.get());
            if (movieName == null) movieName = "Unknown Movie";

            context.write(
                    new Text(movieName),
                    new Text("AverageRating: " + avg + " (TotalRatings: " + count + ")")
            );

            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = movieName;
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            if (maxMovie.equals("")) {
                context.write(
                        new Text("RESULT"),
                        new Text("No movie has at least 5 ratings.")
                );
            } else {
                context.write(
                        new Text("RESULT"),
                        new Text(maxMovie + " is the highest rated movie with an average rating of "
                                + maxRating + " among movies with at least 5 ratings.")
                );
            }
        }
    }

    // ===== Main =====
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai1_Lab1");

        job.setJarByClass(Bai1_Lab1.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Map output types (khác với Reducer output)
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Reducer output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // CHỈ đọc 2 file rating
        FileInputFormat.addInputPath(job, new Path("/input/ratings_1.txt"));
        FileInputFormat.addInputPath(job, new Path("/input/ratings_2.txt"));

        FileOutputFormat.setOutputPath(job, new Path("/output11"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}