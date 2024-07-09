import os
import pydoop.hdfs as hdfs

def upload_images_to_hdfs(local_dir, hdfs_dir):
    if not os.path.isdir(local_dir):
        raise ValueError(f"The local directory {local_dir} does not exist!")

    if not hdfs.path.exists(hdfs_dir):
        hdfs.mkdir(hdfs_dir)

    for filename in os.listdir(local_dir):
        local_path = os.path.join(local_dir, filename)
        hdfs_path = hdfs.path.join(hdfs_dir, filename)
        
        if os.path.isfile(local_path):
            if hdfs.path.exists(hdfs_path):
                print(f"File {hdfs_path} already exists, skipping upload.")
            else:
                hdfs.put(local_path, hdfs_path)
                print(f"Uploaded {local_path} to {hdfs_path}")


if __name__ == "__main__":
    local_directory = "./dataset"
    hdfs_directory = "/dataset"
    upload_images_to_hdfs(local_directory, hdfs_directory)
