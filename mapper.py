#!/usr/bin/python3

import os
import io
import sys
import cv2
import pickle
import numpy as np
from hdfs import InsecureClient

# unpickle CIFAR-10 data from a byte stream
def unpickle(byte_stream):
    dict = pickle.load(io.BytesIO(byte_stream), encoding='bytes')
    return dict

def resize_image(image, new_size):
    return cv2.resize(image, new_size)

def filter_image(label):
    # keep images with label 0 (airplane)
    return label == 0

def noise_reduction(image):
    return cv2.fastNlMeansDenoisingColored(image, None, 10, 10, 7, 21)

def edge_detection(image):
    return cv2.Canny(image, 100, 200)

def sharpen(image):
    kernel = np.array([[0, -1, 0],
                       [-1, 5, -1],
                       [0, -1, 0]])
    return cv2.filter2D(image, -1, kernel)

def blur(image):
    return cv2.GaussianBlur(image, (5, 5), 0)

def main():
    hdfs_client = InsecureClient('http://localhost:9870')
    
    # Resize to 128x128 pixels
    new_size = (128, 128)

    for line in sys.stdin:
        hdfs_path = line.strip()

        try:
            with hdfs_client.read(hdfs_path) as reader:
                byte_stream = reader.read()

            data_dict = unpickle(byte_stream)
            images = data_dict[b'data']
            labels = data_dict[b'labels']
            filenames = data_dict[b'filenames']
            
            processed_images = []
            processed_labels = []

            for idx, img_data in enumerate(images):
                if (labels[idx] != 0):
                    continue
                
                # Convert image data from CIFAR-10 format to 32x32x3 RGB image
                img = np.reshape(img_data, (3, 32, 32)).transpose(1, 2, 0)
                
                resized_img = resize_image(img, new_size)
                img_noise_reduced = noise_reduction(resized_img)
                img_edge_detected = edge_detection(img_noise_reduced)
                img_sharpened = sharpen(img_edge_detected)
                img_blurred = blur(img_sharpened)
                
                if len(img_blurred.shape) == 2:
                    img_blurred = cv2.cvtColor(img_blurred, cv2.COLOR_GRAY2BGR)
                
                # Flatten the processed image to match CIFAR-10 format
                processed_img_flattened = img_blurred.transpose(2, 0, 1).flatten()
                
                processed_images.append(processed_img_flattened)
                processed_labels.append(labels[idx])

                # # Output the processed image as a string with the filename as key
                # _, buffer = cv2.imencode('.jpg', img_blurred)
                # sys.stdout.write(f'{filenames[idx].decode("utf-8")}\t{buffer.tobytes().decode("latin1")}\n')
                print(idx)
                
            # end of for
            print("\n############################## end of for ##############################\n")
            processed_data_dict = {
                b'data': np.array(processed_images),
                b'labels': processed_labels
            }
            
            processed_byte_stream = pickle.dumps(processed_data_dict)
            
            # Output processed data (filename \t binary data)
            # output_key = hdfs_path.split('/')[-1]
            # sys.stdout.write(f"{output_key}\t{processed_byte_stream.decode('latin1')}\n")
            
            # Save processed data back to HDFS
            hdfs_input_path = '/user/tahmine_t/cifar-10'
            hdfs_output_path = '/user/tahmine_t/processed_cifar-10'
            data_files = hdfs_client.list(hdfs_input_path)
            
            for data_file in data_files:
                output_file_path = f"{hdfs_output_path}/{data_file}"
                with hdfs_client.write(output_file_path, overwrite=True) as writer:
                    writer.write(processed_byte_stream)


        except Exception as e:
            sys.stderr.write(f'Error processing file {hdfs_path}: {str(e)}\n')


if __name__ == "__main__":
    main()
