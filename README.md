# Cloud Computing Project with Hadoop

This project leverages Hadoop for distributed processing of the CIFAR-10 dataset, performing various image processing tasks.

## Project Overview

### Dataset
- Dataset: CIFAR-10
- Location: Uploaded on the cloud

### Hadoop Components
- Mapper: Reads data from the cloud and performs initial processing.
- Reducer: Completes the processing tasks and outputs the results.

### Image Processing Tasks
- Image Resizing
- Filtering
- Noise Reduction
- Edge Detection
- Sharpening
- Blurring

## Getting Started

1. Setup Hadoop: Ensure Hadoop is installed and configured on your cluster.
2. Upload CIFAR-10 Dataset: Make sure the CIFAR-10 dataset is available on the cloud storage accessible by Hadoop.
3. Run Hadoop Job:
  
   hadoop jar your-hadoop-job.jar input_path output_path
   
4. View Results: Processed images will be available in the specified output path.

## Authors
Tahmine Tavakoli, Elham Armin, Alireza Amini
