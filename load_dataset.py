import os
import io
import cv2
import pickle
import numpy as np
from hdfs import InsecureClient

# HDFS configuration
hdfs_url = 'http://localhost:9870'
hdfs_input_path = '/user/tahmine_t/processed_cifar-10'
local_output_path = './dataset/processed_cifar-10'

os.makedirs(local_output_path, exist_ok=True)

client = InsecureClient(hdfs_url)

def unpickle(byte_stream):
    dict = pickle.load(io.BytesIO(byte_stream), encoding='bytes')
    return dict

data_files = client.list(hdfs_input_path)

image_count = 0
max_images = 100

for data_file in data_files:
    if image_count >= max_images:
        break
    
    hdfs_file_path = f"{hdfs_input_path}/{data_file}"

    with client.read(hdfs_file_path) as reader:
        byte_stream = reader.read()
        data_dict = unpickle(byte_stream)
    
    images = data_dict[b'data']
    labels = data_dict[b'labels']

    for idx, img_data in enumerate(images):
        if image_count >= max_images:
            break
        
        # Convert image data from CIFAR-10 format to 32x32x3 RGB image
        img = np.reshape(img_data, (3, 128, 128)).transpose(1, 2, 0)
        img_bgr = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
        
        output_file_path = os.path.join(local_output_path, f"image_{image_count}.jpg")
        cv2.imwrite(output_file_path, img_bgr)
        
        image_count += 1

print(f"Saved {image_count} images to {local_output_path}.")
