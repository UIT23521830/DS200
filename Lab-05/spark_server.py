import sys
import os
import glob
spark_home = os.path.expanduser('~/spark')
try:
    sys.path.insert(0, f"{spark_home}/python")
    sys.path.insert(0, glob.glob(f"{spark_home}/python/lib/py4j-*-src.zip")[0])
except Exception:
    pass

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

def detect_people(image_base64):
    try:
        import cv2
        import numpy as np
        import base64
        import json
        import os
        
        # Giải mã ảnh
        decoded_data = base64.b64decode(image_base64)
        np_data = np.frombuffer(decoded_data, np.uint8)
        img = cv2.imdecode(np_data, cv2.IMREAD_COLOR)
        
        if img is not None:
            box_list = []
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

            # 1. Nhận diện khuôn mặt (Cực kỳ nhạy khi bạn ngồi trước laptop)
            face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
            faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))
            for (x, y, w, h) in faces:
                cv2.rectangle(img, (x, y), (x+w, y+h), (255, 0, 0), 2) # Xanh dương cho mặt
                box_list.append({"type": "face", "x": int(x), "y": int(y), "w": int(w), "h": int(h)})

            # 2. Nhận diện toàn thân bằng HOG (Dùng cho người đứng xa)
            hog = cv2.HOGDescriptor()
            hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())
            (boxes, _) = hog.detectMultiScale(img, winStride=(8, 8), padding=(16, 16), scale=1.05)
            for (x, y, w, h) in boxes:
                cv2.rectangle(img, (x, y), (x+w, y+h), (0, 255, 0), 2) # Xanh lá cho thân
                box_list.append({"type": "person", "x": int(x), "y": int(y), "w": int(w), "h": int(h)})
            
            # LƯU ẢNH TRỰC QUAN ĐỂ KIỂM TRA
            os.makedirs("kiem_tra_hinh_anh", exist_ok=True)
            cv2.imwrite("kiem_tra_hinh_anh/ket_qua_nhan_dien_moi_nhat.jpg", img)
            
            # Trả về tọa độ JSON Bounding Box chuẩn xác theo yêu cầu đề bài
            return json.dumps(box_list)
        return "[]"
    except Exception as e:
        return "[]"

def main():
    print("Khởi động Spark Server (Đóng vai trò Server 2 xử lý)...")
    spark = SparkSession.builder \
        .appName("CameraStreaming") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    # Khai báo UDF trả về Text JSON (vì tọa độ có nhiều điểm)
    detect_udf = udf(detect_people, StringType())
    
    schema = StructType([
        StructField("camera_id", StringType(), True),
        StructField("frame_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("image_data", StringType(), True)
    ])

    print("Đang nghe ngóng dữ liệu từ Camera...")
    stream_df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 6100) \
        .load()

    # Nhúng AI vào Spark
    parsed_df = stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
    processed_df = parsed_df.withColumn("bounding_boxes", detect_udf(col("image_data")))
    
    # Dọn dẹp bảng
    result_df = processed_df.drop("image_data").withColumn("processed_time", current_timestamp())

    print("\nHệ thống đang chạy 🚀")
    print("Mở thư mục 'kiem_tra_hinh_anh' để xem AI vẽ khung nhận diện.")
    print("Dữ liệu Bounding Box (Server 3) đang được ghi vào thư mục: 'server_3_luu_tru'")
    
    # SERVER 3 CHÍNH THỨC NẰM Ở ĐÂY: Ghi xuống mạng HDFS
    query = result_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "hdfs://localhost:9000/lab05/server_3_luu_tru/") \
        .option("checkpointLocation", "hdfs://localhost:9000/lab05/checkpoints/") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
