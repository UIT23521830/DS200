import socket
import json
import time
import base64
import numpy as np
import cv2

class Config:
    host = "localhost"
    port = 6100

def connect_tcp():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((Config.host, Config.port))
    s.listen(1)
    
    print(f"Đang chờ kết nối ở cổng {Config.port}...")
    connection, address = s.accept()
    print(f"Đã kết nối với: {address}")
    
    return connection

def main():
    tcp_connection = connect_tcp()
    
    # Thử Camera index 0, nếu không được thì thử index 1
    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        cap = cv2.VideoCapture(1)
    use_dummy = not cap.isOpened()
    
    if use_dummy:
        print("Không mở được camera. Sẽ gửi ảnh mẫu.")

    print("Bắt đầu phát dữ liệu...")
    try:
        frame_id = 0
        while True:
            if use_dummy:
                frame = np.ones((480, 640, 3), dtype=np.uint8) * 100
            else:
                ret, frame = cap.read()
                if not ret:
                    print("Mất kết nối camera.")
                    break
                    
            frame_id += 1
            
            # Giảm kích thước ảnh để đỡ nặng mạng
            frame = cv2.resize(frame, (640, 480))
            _, buffer = cv2.imencode('.jpg', frame)
            encoded_img = base64.b64encode(buffer).decode('utf-8')
            
            payload = {
                "camera_id": "cam_01",
                "frame_id": frame_id,
                "timestamp": str(time.time()),
                "image_data": encoded_img
            }
            
            # Gửi dữ liệu kèm \n để Spark dễ ngắt dòng
            data_to_send = (json.dumps(payload) + "\n").encode('utf-8')
            tcp_connection.send(data_to_send)
            
            print(f"Gửi frame {frame_id}")
            time.sleep(1)
            
    except Exception as e:
        print(f"Đã ngắt kết nối: {e}")
    finally:
        cap.release()
        tcp_connection.close()

if __name__ == "__main__":
    main()
