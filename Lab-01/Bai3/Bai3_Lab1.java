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

public class Bai3_Lab1 {

    // ================= MAPPER =================
    // Đọc ratings.txt (Dữ liệu lớn)
    // Lookup giới tính từ users.txt (Dữ liệu nhỏ - nạp vào HashMap)
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        private HashMap<Integer, String> userGenderMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Nạp dữ liệu users.txt vào HashMap để tra cứu nhanh
            Path path = new Path("/input/users.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            
            String line;
            while ((line = br.readLine()) != null) {
                // Định dạng: UserID, Gender, Age, Occupation, Zip-code
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    try {
                        int userId = Integer.parseInt(parts[0].trim());
                        String gender = parts[1].trim(); // "M" hoặc "F"
                        userGenderMap.put(userId, gender);
                    } catch (NumberFormatException e) { }
                }
            }
            br.close();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Định dạng ratings: UserID, MovieID, Rating, Timestamp
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            
            String[] parts = line.split(",");
            if (parts.length < 3) return;

            // Bỏ qua dòng Header
            if (!parts[0].trim().matches("-?\\d+")) return;

            try {
                int userId = Integer.parseInt(parts[0].trim());
                int movieId = Integer.parseInt(parts[1].trim());
                double rating = Double.parseDouble(parts[2].trim());

                String gender = userGenderMap.get(userId);
                if (gender != null) {
                    // Key: MovieID
                    // Value: "GiớiTính:Điểm" (Ví dụ: "M:4.0")
                    context.write(new IntWritable(movieId), new Text(gender + ":" + rating));
                }
            } catch (NumberFormatException e) { }
        }
    }

    // ================= REDUCER =================
    // Tính AVG Rating của từng giới tính cho mỗi phim
    public static class MyReducer extends Reducer<IntWritable, Text, Text, Text> {

        private HashMap<Integer, String> movieTitlesMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Nạp movies.txt vào HashMap để lấy tên phim
            Path path = new Path("/input/movies.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            
            String line;
            while ((line = br.readLine()) != null) {
                // Định dạng: MovieID, Title, Genres
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
            double sumM = 0; int countM = 0;
            double sumF = 0; int countF = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                String gender = parts[0];
                double rating = Double.parseDouble(parts[1]);

                if (gender.equalsIgnoreCase("M")) {
                    sumM += rating; countM++;
                } else if (gender.equalsIgnoreCase("F")) {
                    sumF += rating; countF++;
                }
            }

            String movieTitle = movieTitlesMap.getOrDefault(key.get(), "Unknown Movie");

            // Tạo chuỗi hiển thị kết quả
            String maleRes = (countM > 0) ? String.format("%.2f, Count: %d", (sumM / countM), countM) : "N/A";
            String femaleRes = (countF > 0) ? String.format("%.2f, Count: %d", (sumF / countF), countF) : "N/A";

            String output = String.format("Male Avg: %s | Female Avg: %s", maleRes, femaleRes);
            context.write(new Text(movieTitle), new Text(output));
        }
    }

    // ================= MAIN =================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Rating By Gender");
        job.setJarByClass(Bai3_Lab1.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Kiểu dữ liệu trung gian (Map output)
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Kiểu dữ liệu đầu ra cuối cùng
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Chỉ định file input và output
        FileInputFormat.addInputPath(job, new Path("/input/ratings_1.txt"));
        FileInputFormat.addInputPath(job, new Path("/input/ratings_2.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/output13"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
