import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai2_Lab1 {

    // ================= MAPPER =================
    // Đầu vào là file ratings.txt
    // Cần movies.txt để biết ID phim thuộc thể loại nào
    public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        // Bảng lưu mapping: MovieID -> Danh sách các thể loại
        private HashMap<Integer, String[]> movieGenresMap = new HashMap<>();

        // Hàm setup được gọi 1 lần khi bắt đầu Mapper
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Đọc file movies.txt từ HDFS
            Path pt = new Path("/input/movies.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            
            String line;
            while ((line = br.readLine()) != null) {
                // File movies.txt có dạng: MovieID, Title, Genres
                // Ví dụ: 1043, Toy Story (1995), Animation|Children|Comedy
                String[] parts = line.split(",");
                if (parts.length >= 3) {
                    try {
                        int movieId = Integer.parseInt(parts[0].trim());
                        // Cắt các thể loại ngăn cách bởi dấu |
                        // Cần dùng "\\|" vì | là ký tự đặc biệt trong Regex
                        String[] genres = parts[2].trim().split("\\|");
                        
                        movieGenresMap.put(movieId, genres);
                    } catch (NumberFormatException e) {
                        // Bỏ qua nếu lỗi format số
                    }
                }
            }
            br.close();
        }

        // Hàm map xử lý từng dòng của ratings.txt
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // File ratings.txt có dạng: UserID, MovieID, Rating, Timestamp
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 3) return;

            // Kiểm tra xem dòng đầu có phải là tên cột (header) không, nếu có thì bỏ qua
            if (!parts[0].trim().matches("-?\\d+")) return;

            try {
                int movieId = Integer.parseInt(parts[1].trim());
                double rating = Double.parseDouble(parts[2].trim());

                // Lấy ra danh sách thể loại của bộ phim này
                String[] genres = movieGenresMap.get(movieId);
                
                // Nếu phim có trong danh sách thể loại
                if (genres != null) {
                    // Xuất ra <Thể loại, Rating> cho mỗi thể loại phim
                    for (String genre : genres) {
                        // Ví dụ: <"Action", 4.5>, <"Sci-Fi", 4.5>
                        context.write(new Text(genre.trim()), new DoubleWritable(rating));
                    }
                }
            } catch (NumberFormatException e) {
                // Bỏ qua dòng bị lỗi
            }
        }
    }

    // ================= REDUCER =================
    // Đầu vào của Reducer là các cặp <Thể Loại, Danh sách các điểm Rating>
    public static class MyReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            // Tính tổng điểm và đếm số lượt đánh giá
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            // Chỉ tính nếu có ít nhất 1 lượt rating để tránh chia cho 0
            if (count > 0) {
                double avg = sum / count;
                
                // Tạo chuỗi kết quả theo định dạng yêu cầu: Avg: xx, Count xx
                // Dùng String.format("%.2f") để làm tròn 2 chữ số thập phân
                String result = String.format("Avg: %.2f, Count: %d", avg, count);
                
                // Viất ra file kết quả
                // Key = Genre (Thể loại)
                // Value = Avg: xx, Count xx
                context.write(key, new Text(result));
            }
        }
    }

    // ================= KÀM MAIN =================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Đặt tên cho công việc là Bai2_Lab1
        Job job = Job.getInstance(conf, "Bai2_Lab1");
        job.setJarByClass(Bai2_Lab1.class);

        // Khai báo lớp Mapper và Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Khai báo kiểu dữ liệu output của Mapper (Khác với output của Reducer)
        // Mapper xuất ra <Text, DoubleWritable>
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Khai báo kiểu dữ liệu output của Reducer (Và của toàn bộ Job)
        // Reducer xuất ra <Text, Text>
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Thêm đường dẫn file input (ratings_1 và ratings_2)
        FileInputFormat.addInputPath(job, new Path("/input/ratings_1.txt"));
        FileInputFormat.addInputPath(job, new Path("/input/ratings_2.txt"));
        
        // Đặt đường dẫn file output (Lưu ý: folder này phải chưa tồn tại trên HDFS)
        FileOutputFormat.setOutputPath(job, new Path("/output_bai2"));

        // Chạy job và chờ khi nào xong thì thoát
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
