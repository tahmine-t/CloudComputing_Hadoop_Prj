import os
import cv2
import numpy as np
import pydoop.hdfs as hdfs

# Function to read image from HDFS, resize and save locally
def process_image(hdfs_path, local_save_dir):
    filename = os.path.basename(hdfs_path)
    local_save_path = os.path.join(local_save_dir, filename)

    with hdfs.open(hdfs_path) as f:
        img_array = np.asarray(bytearray(f.read()), dtype=np.uint8)
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        if img is not None:
            resized_img = cv2.resize(img, new_size)
            cv2.imwrite(local_save_path, resized_img)
        else:
            print(f"Failed to load image: {hdfs_path}")


if __name__ == "__main__":
    hdfs_dir = '/dataset'
    local_save_dir = './resized_results'

    if not hdfs.path.exists(hdfs_dir):
        raise ValueError(f"The directory {hdfs_dir} does not exist!")

    if not os.path.exists(local_save_dir):
        os.makedirs(local_save_dir)
        
    # new size for resizing
    new_size = (128, 128)

    hdfs_files = hdfs.ls(hdfs_dir)

    for hdfs_file in hdfs_files:
        process_image(hdfs_file, local_save_dir)
