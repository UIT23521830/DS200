import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai4_Lab1 {

    // ================= MAPPER =================
    // Phát ra: MovieID -> Age:Rating
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        private HashMap<Integer, Integer> userAgeMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            Path path = new Path("/input/users.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 3) {
                    try {
                        int userId = Integer.parseInt(parts[0].trim());
                        int age = Integer.parseInt(parts[2].trim());
                        userAgeMap.put(userId, age);
                    } catch (NumberFormatException e) { }
                }
            }
            br.close();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            
            String[] parts = line.split(",");
            if (parts.length < 3) return;
            if (!parts[0].trim().matches("-?\\d+")) return;

            try {
                int userId = Integer.parseInt(parts[0].trim());
                int movieId = Integer.parseInt(parts[1].trim());
                double rating = Double.parseDouble(parts[2].trim());

                Integer age = userAgeMap.get(userId);
                if (age != null) {
                    // Emit: MovieID, value là "Tuổi:Điểm"
                    context.write(new IntWritable(movieId), new Text(age + ":" + rating));
                }
            } catch (NumberFormatException e) { }
        }
    }

    // ================= REDUCER =================
    // Tính AVG cho 4 nhóm tuổi trên mỗi phim
    public static class MyReducer extends Reducer<IntWritable, Text, Text, Text> {

        private HashMap<Integer, String> movieTitlesMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            Path path = new Path("/input/movies.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    try {
                        int mid = Integer.parseInt(parts[0].trim());
                        String title = parts[1].trim();
                        movieTitlesMap.put(mid, title);
                    } catch (NumberFormatException e) { }
                }
            }
            br.close();
        }

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Khai báo sum và count cho 4 nhóm
            double[] sums = new double[4]; // 0: 0-18, 1: 18-35, 2: 35-50, 3: 50+
            int[] counts = new int[4];

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                int age = Integer.parseInt(parts[0]);
                double rating = Double.parseDouble(parts[1]);

                // Phân nhóm
                if (age <= 18) {
                    sums[0] += rating; counts[0]++;
                } else if (age > 18 && age <= 35) {
                    sums[1] += rating; counts[1]++;
                } else if (age > 35 && age <= 50) {
                    sums[2] += rating; counts[2]++;
                } else {
                    sums[3] += rating; counts[3]++;
                }
            }

            String movieTitle = movieTitlesMap.getOrDefault(key.get(), "Unknown Movie");

            // Format kết quả cho 4 nhóm
            String g1 = (counts[0] > 0) ? String.format("%.2f", sums[0]/counts[0]) : "NA";
            String g2 = (counts[1] > 0) ? String.format("%.2f", sums[1]/counts[1]) : "NA";
            String g3 = (counts[2] > 0) ? String.format("%.2f", sums[2]/counts[2]) : "NA";
            String g4 = (counts[3] > 0) ? String.format("%.2f", sums[3]/counts[3]) : "NA";

            // VD: MovieTitle 0-18: NA 18-35: 3.75 35-50: NA 50+: NA
            String output = String.format("0-18: %s 18-35: %s 35-50: %s 50+: %s", g1, g2, g3, g4);
            context.write(new Text(movieTitle), new Text(output));
        }
    }

    // ================= MAIN =================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating By Age Groups");
        job.setJarByClass(Bai4_Lab1.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/input/ratings_1.txt"));
        FileInputFormat.addInputPath(job, new Path("/input/ratings_2.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/output14"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
